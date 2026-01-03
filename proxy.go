package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	log "sentioxyz/sentio-core/common/log"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	activeConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_proxy_active_connections",
		Help: "Number of currently active client connections",
	})
	packetsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_packets_total",
		Help: "Total count of ClickHouse protocol packets processed",
	}, []string{"type"})
	bytesTransferred = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_bytes_transferred_total",
		Help: "Total bytes transferred through the proxy",
	}, []string{"direction"})
)

func init() {
	prometheus.MustRegister(activeConnections)
	prometheus.MustRegister(packetsTotal)
	prometheus.MustRegister(bytesTransferred)
}

// Known client -> server packet types in ClickHouse native protocol.
// Values match Protocol::Client enum in ClickHouse sources.
var packetNames = map[uint64]string{
	0: "Hello",
	1: "Query",
	2: "Data",
	3: "Cancel",
	4: "Ping",
	5: "TablesStatusRequest",
	6: "KeepAlive",
	7: "Scalar",
	8: "Poll",
	9: "Data (portable)",
}

// Build reverse lookup for known packet names to avoid double-printing.
var packetNamesByName = func() map[string]struct{} {
	m := make(map[string]struct{}, len(packetNames))
	for _, v := range packetNames {
		m[v] = struct{}{}
	}
	return m
}()

type packetStats struct {
	mu     sync.Mutex
	counts map[string]int64
}

func newPacketStats() *packetStats {
	return &packetStats{counts: make(map[string]int64)}
}

func (s *packetStats) inc(name string) {
	s.mu.Lock()
	s.counts[name]++
	s.mu.Unlock()
}

func (s *packetStats) snapshot() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.counts))
	for k, v := range s.counts {
		out[k] = v
	}
	return out
}

type proxy struct {
	cfg       Config
	stats     *packetStats
	validator Validator
}

func newProxy(cfg Config, v Validator) *proxy {
	if v == nil {
		v = NoopValidator{}
	}
	return &proxy{
		cfg:       cfg,
		stats:     newPacketStats(),
		validator: v,
	}
}

// countingReader wraps an io.Reader and counts bytes read.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// queryParser incrementally decodes Hello/Query packets to extract accurate SQL bodies.
// It assumes the buffer starts on a packet boundary; for unknown packet types it drops the buffer.
type queryParser struct {
	version      int
	buf          []byte
	addendumDone bool
	disabled     bool // if true, stop parsing forever on this connection

}

// maxParserBufSize limits the parser buffer to prevent memory exhaustion.
// Query packets should be small; if buffer exceeds this, we discard and reset.
const maxParserBufSize = 1 << 20 // 1MB

// resetBuf releases the underlying buffer memory to GC.
func (p *queryParser) resetBuf() {
	p.buf = nil
}

// consumeBuf removes the first n bytes from the buffer.
// It copies remaining data to a new slice to allow GC of the old underlying array.
func (p *queryParser) consumeBuf(n int) {
	if n >= len(p.buf) {
		p.buf = nil
		return
	}
	remaining := make([]byte, len(p.buf)-n)
	copy(remaining, p.buf[n:])
	p.buf = remaining
}

// skipAddendum attempts to consume the optional "addendum" section that
// follows the Hello/handshake in newer protocol versions. It returns:
//   - consumed > 0 and ok=true when the addendum was fully read,
//   - ok=false when more bytes are needed (no state is changed),
//   - err on hard parse errors.
func (p *queryParser) skipAddendum() (consumed int, ok bool, err error) {
	// The addendum layout mirrors Connection::sendAddendum in ClickHouse:
	//   if (server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY)
	//       writeStringBinary(quota_key)
	//   if (server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS)
	//       writeStringBinary(proto_send_chunked)
	//       writeStringBinary(proto_recv_chunked)
	//   if (server_revision >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL)
	//       writeVarUInt(DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION)
	//
	// We don't need the values themselves, only their lengths, so work
	// directly on the raw buffer to avoid interference from bufio.Reader
	// prefetching.
	buf := p.buf
	offset := 0

	readString := func() (bool, error) {
		l, n := binary.Uvarint(buf[offset:])
		if n <= 0 {
			return false, nil
		}
		if len(buf[offset+n:]) < int(l) {
			return false, nil
		}
		offset += n + int(l)
		return true, nil
	}
	readUVar := func() (bool, error) {
		_, n := binary.Uvarint(buf[offset:])
		if n <= 0 {
			return false, nil
		}
		offset += n
		return true, nil
	}

	if proto.FeatureQuotaKey.In(p.version) {
		ok, err = readString()
		if err != nil {
			return 0, false, err
		}
		if !ok {
			return 0, false, nil
		}
	}
	if proto.FeatureChunkedPackets.In(p.version) {
		ok, err = readString()
		if err != nil {
			return 0, false, err
		}
		if !ok {
			return 0, false, nil
		}
		ok, err = readString()
		if err != nil {
			return 0, false, err
		}
		if !ok {
			return 0, false, nil
		}
	}
	if proto.FeatureVersionedParallelReplicas.In(p.version) {
		ok, err = readUVar()
		if err != nil {
			return 0, false, err
		}
		if !ok {
			return 0, false, nil
		}
	}

	return offset, true, nil
}

type ParsedQuery struct {
	Body      string
	Signature string
}

func decodeQueryBody(data []byte, version int, forceSettings bool) (string, string, int, error) {
	cr := &countingReader{r: bytes.NewReader(data)}
	r := proto.NewReader(cr)

	// QueryID
	if _, err := r.Str(); err != nil {
		return "", "", cr.n, err
	}

	var quotaKey string

	if proto.FeatureClientWriteInfo.In(version) {
		var info proto.ClientInfo
		if err := info.DecodeAware(r, version); err != nil {
			return "", "", cr.n, err
		}
		quotaKey = info.QuotaKey
	}

	if !proto.FeatureSettingsSerializedAsStrings.In(version) && !forceSettings {
		return "", "", cr.n, errors.New("settings not serialized as strings")
	}

	for {
		var s proto.Setting
		if err := s.Decode(r); err != nil {
			return "", "", cr.n, err
		}
		if s.Key == "" {
			break
		}
	}

	if proto.FeatureInterserverExternallyGrantedRoles.In(version) {
		if _, err := r.Str(); err != nil {
			return "", "", cr.n, err
		}
	}

	if proto.FeatureInterServerSecret.In(version) {
		if _, err := r.Str(); err != nil {
			return "", "", cr.n, err
		}
	}

	if _, err := r.UVarInt(); err != nil { // Stage
		return "", "", cr.n, err
	}
	if _, err := r.UVarInt(); err != nil { // Compression
		return "", "", cr.n, err
	}

	body, err := r.Str()
	if err != nil {
		return "", "", cr.n, err
	}

	if proto.FeatureParameters.In(version) {
		for {
			var p proto.Parameter
			if err := p.Decode(r); err != nil {
				return "", "", cr.n, err
			}
			if p.Key == "" {
				break
			}
		}
	}

	return body, quotaKey, cr.n, nil
}

func (p *queryParser) feed(chunk []byte) ([]ParsedQuery, error) {
	if p.disabled {
		return nil, nil
	}

	p.buf = append(p.buf, chunk...)

	// Safety limit: if buffer exceeds max size, discard to prevent OOM.
	// This can happen with very large Query packets or malformed data.
	if len(p.buf) > maxParserBufSize {
		p.resetBuf()
		p.disabled = true
		return nil, errors.New("parser buffer exceeded max size, discarding, parser disabled")
	}

	var out []ParsedQuery
	var decodeErr error
	for {
		// After we know the protocol version, ClickHouse will send a single
		// "addendum" block as part of the handshake (quota key, chunked
		// protocol negotiation, parallel replicas). This block does not
		// start with a packet type varint, so if it gets coalesced with the
		// first Query into a single TCP read, naive parsing will never see
		// the Query. Explicitly skip addendum once per connection.
		if p.version != 0 && !p.addendumDone && proto.FeatureAddendum.In(p.version) {
			consumed, ok, err := p.skipAddendum()
			if err != nil {
				decodeErr = err
				p.resetBuf()
				p.disabled = true
				return out, decodeErr
			}
			if !ok {
				// Need more bytes to complete addendum.
				return out, decodeErr
			}
			p.addendumDone = true
			p.consumeBuf(consumed)
			if len(p.buf) == 0 {
				return out, decodeErr
			}
		}

		typ, n := binary.Uvarint(p.buf)
		if n <= 0 {
			return out, decodeErr
		}

		switch typ {
		case 0: // Hello
			cr := &countingReader{r: bytes.NewReader(p.buf[n:])}
			r := proto.NewReader(cr)
			var hello proto.ClientHello
			if err := hello.Decode(r); err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
					return out, decodeErr
				}
				decodeErr = err
				p.resetBuf()
				p.disabled = true
				return out, decodeErr
			}
			log.Infof("Detected ClientHello. Version: %d", hello.ProtocolVersion)
			p.version = hello.ProtocolVersion
			consumed := n + cr.n
			p.consumeBuf(consumed)
		case 1: // Query
			if p.version == 0 {
				p.resetBuf()
				p.disabled = true
				return out, decodeErr
			}
			// We need QuotaKey from ClientInfo. ch-go/proto.Query struct doesn't expose it
			// easily (or at all in this version), so we use our own decoder (decodeQueryBody).
			
			// 1. Try strict decode (forceSettings=false)
			// We reset the reader for the second attempt if needed, but since we have the buffer,
			// we can just call decodeQueryBody on the buffer slice.
			
			log.Infof("Attempting strict decode of Query. buffer len=%d", len(p.buf[n:]))
			body, quotaKey, consumed, err := decodeQueryBody(p.buf[n:], p.version, false)
			log.Infof("Strict decode result: err=%v, len(body)=%d, consumed=%d, quotaKey=%s", err, len(body), consumed, quotaKey)
			if err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
					return out, decodeErr
				}
				
				// 2. Try permissive decode (forceSettings=true)
				log.Infof("Attempting permissive decode of Query")
				body2, quotaKey2, consumed2, err2 := decodeQueryBody(p.buf[n:], p.version, true)
				log.Infof("Permissive decode result: err=%v, len(body)=%d, consumed=%d, quotaKey=%s", err2, len(body2), consumed2, quotaKey2)
				if err2 == nil {
					out = append(out, ParsedQuery{Body: body2, Signature: quotaKey2})
					p.consumeBuf(n + consumed2)
					continue
				}
				
				decodeErr = err
				p.resetBuf()
				p.disabled = true
				return out, decodeErr
			}
			
			// Strict decode succeeded
			out = append(out, ParsedQuery{Body: body, Signature: quotaKey})
			p.consumeBuf(n + consumed)
		default:
			// Unknown packet type (e.g., Data); reset to release memory.
			p.resetBuf()
			p.disabled = true
			return out, decodeErr
		}
	}
}

// detectPacketType tries to read a ClickHouse packet type from the
// beginning of a chunk. This is intentionally best-effort: it only looks
// at the first varint in the buffer and does not attempt full protocol
// parsing. If detection fails we return "unknown".
func detectPacketType(chunk []byte) string {
	if len(chunk) == 0 {
		return "unknown"
	}

	// Packet type is a varint; for common packets it fits into a single byte
	// with high bit unset. If the first byte has the continuation bit set,
	// treat it as payload, not a packet boundary.
	if chunk[0]&0x80 != 0 {
		return "unknown"
	}

	typ, n := binary.Uvarint(chunk)
	if n <= 0 {
		return "unknown"
	}
	if name, ok := packetNames[typ]; ok {
		return name
	}
	// Count small unknown ids to help spot unexpected packets; otherwise
	// group as generic unknown.
	if typ < 32 {
		return fmt.Sprintf("type_%d", typ)
	}
	return "unknown"
}

func (p *proxy) serve(ctx context.Context) error {
	lc := net.ListenConfig{KeepAlive: 30 * time.Second}
	ln, err := lc.Listen(ctx, "tcp", p.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen error: %w", err)
	}
	defer ln.Close()

	if p.cfg.StatsInterval.Duration > 0 {
		go p.runStatsPrinter(ctx)
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	var connID int64
	for {
		clientConn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				printStats(p.stats)
				return nil
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Infof("accept error: %v", err)
			continue
		}

		id := atomic.AddInt64(&connID, 1)
		log.Infof("[conn %d] new connection from %s", id, clientConn.RemoteAddr())

		go p.handleConnection(ctx, id, clientConn)
	}
}

func (p *proxy) runStatsPrinter(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.StatsInterval.Duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			printStats(p.stats)
		case <-ctx.Done():
			printStats(p.stats)
			return
		}
	}
}

func (p *proxy) handleConnection(ctx context.Context, id int64, clientConn net.Conn) {
	activeConnections.Inc()
	defer activeConnections.Dec()
	defer clientConn.Close()
	if tc, ok := clientConn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
	}

	dialer := &net.Dialer{
		Timeout:   p.cfg.DialTimeout.Duration,
		KeepAlive: 30 * time.Second,
	}

	upstreamCtx := ctx
	var cancel context.CancelFunc
	if p.cfg.DialTimeout.Duration > 0 {
		upstreamCtx, cancel = context.WithTimeout(ctx, p.cfg.DialTimeout.Duration)
	} else {
		upstreamCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	upstreamConn, err := dialer.DialContext(upstreamCtx, "tcp", p.cfg.Upstream)
	if err != nil {
		log.Infof("[conn %d] dial upstream %s error: %v", id, p.cfg.Upstream, err)
		return
	}
	defer upstreamConn.Close()
	if tc, ok := upstreamConn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
	}

	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			clientConn.Close()
			upstreamConn.Close()
		})
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		p.copyUpstreamToClient(id, clientConn, upstreamConn)
		closeBoth()
	}()

	go func() {
		defer wg.Done()
		p.copyClientToUpstream(ctx, id, clientConn, upstreamConn)
		closeBoth()
	}()

	wg.Wait()
	log.Infof("[conn %d] closed", id)
}

func (p *proxy) copyUpstreamToClient(id int64, clientConn, upstreamConn net.Conn) {
	buf := make([]byte, 64*1024)
	for {
		if p.cfg.IdleTimeout.Duration > 0 {
			_ = upstreamConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}
		n, err := upstreamConn.Read(buf)
		if n > 0 {
			bytesTransferred.WithLabelValues("upstream_to_client").Add(float64(n))
			if p.cfg.IdleTimeout.Duration > 0 {
				_ = clientConn.SetWriteDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
			}
			if _, werr := clientConn.Write(buf[:n]); werr != nil {
				log.Infof("[conn %d] upstream->client write error: %v", id, werr)
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !isTimeout(err) && !errors.Is(err, net.ErrClosed) {
				log.Infof("[conn %d] upstream->client read error: %v", id, err)
			}
			return
		}
	}
}

func (p *proxy) copyClientToUpstream(ctx context.Context, id int64, clientConn, upstreamConn net.Conn) {
	buf := make([]byte, 64*1024)
	parser := &queryParser{}
	for {
		if p.cfg.IdleTimeout.Duration > 0 {
			_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}
		n, readErr := clientConn.Read(buf)
		if n > 0 {
			log.Infof("Proxy read %d bytes from client. Chunk hex: %x", n, buf[:n])
			chunk := buf[:n]
			pkt := detectPacketType(chunk)
			p.stats.inc(pkt)
			packetsTotal.WithLabelValues(pkt).Inc()
			bytesTransferred.WithLabelValues("client_to_upstream").Add(float64(n))

			// Feed all chunks to parser to capture Hello + Query accurately.
			// Parsed SQL will be validated through the Validator.
			sqls, perr := parser.feed(chunk)
			if perr != nil {
				log.Infof("[conn %d] query decode warning: %v", id, perr)
			}
			for _, q := range sqls {
				meta := QueryMeta{
					ConnID:       id,
					ClientAddr:   clientConn.RemoteAddr().String(),
					UpstreamAddr: p.cfg.Upstream,
					QueryPreview: q.Body,
					Raw:          append([]byte(nil), chunk...),
					SQL:          q.Body,
					Signature:    q.Signature,
				}
				if err := p.validator.ValidateQuery(ctx, meta); err != nil {
					log.Infof("[conn %d] query rejected: %v", id, err)
					return
				}
				if p.cfg.LogQueries {
					log.Infof("[conn %d %s -> %s] Query: [%s]", id, clientConn.RemoteAddr(), p.cfg.Upstream, q.Body)
					log.Infof("[conn %d %s -> %s] Query raw hex: % X", id, clientConn.RemoteAddr(), p.cfg.Upstream, []byte(q.Body))
				}
			}

			if p.cfg.LogData && pkt == "Data" {
				p.logPacket(id, clientConn.RemoteAddr().String(), pkt, chunk)
			}

			if p.cfg.IdleTimeout.Duration > 0 {
				_ = upstreamConn.SetWriteDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
			}
			if _, err := upstreamConn.Write(chunk); err != nil {
				log.Infof("[conn %d] client->upstream write error: %v", id, err)
				return
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && !isTimeout(readErr) && !errors.Is(readErr, net.ErrClosed) {
				log.Infof("[conn %d] client->upstream read error: %v", id, readErr)
			}
			return
		}
	}
}

func (p *proxy) logPacket(id int64, clientAddr string, pktType string, chunk []byte) {
	switch pktType {
	case "Query":
		if p.cfg.LogQueries {
			summary := extractQuerySummary(chunk, p.cfg.MaxQueryLogBytes)
			log.Infof("[conn %d %s -> %s] Query: [%s]", id, clientAddr, p.cfg.Upstream, summary)
		}
	case "Data":
		if p.cfg.LogData {
			log.Infof("[conn %d %s -> %s] Data packet (%d bytes): %s", id, clientAddr, p.cfg.Upstream, len(chunk), summarizePrintable(chunk, p.cfg.MaxDataLogBytes))
		}
	default:
	}
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		return true
	}
	return false
}

func printStats(stats *packetStats) {
	snap := stats.snapshot()
	log.Infof("==== ck_remote_proxy stats ====")
	for _, key := range []string{"Hello", "Query", "Data", "Ping", "Cancel", "TablesStatusRequest", "KeepAlive", "Scalar", "Poll", "Data (portable)", "unknown"} {
		log.Infof("%-18s: %d", key, snap[key])
	}
	// Print any extra ids that appeared.
	for k, v := range snap {
		if _, known := packetNamesByName[k]; known {
			continue
		}
		if k == "unknown" {
			continue
		}
		log.Infof("%-18s: %d", k, v)
	}
	log.Infof("===============================")
}

// summarizePrintable extracts a compact ASCII summary from raw bytes, replacing
// non-printable chars with space and collapsing whitespace.
func summarizePrintable(b []byte, maxLen int) string {
	if len(b) == 0 {
		return ""
	}
	var buf bytes.Buffer
	limit := len(b)
	if limit > 1024 {
		limit = 1024
	}
	spaces := 0
	for i := 0; i < limit && buf.Len() < maxLen; i++ {
		c := b[i]
		if c >= 32 && c <= 126 && c != ' ' {
			if spaces > 0 && buf.Len() > 0 {
				buf.WriteByte(' ')
				spaces = 0
			}
			buf.WriteByte(c)
		} else {
			spaces++
		}
	}
	return buf.String()
}

// extractQuerySummary cleans a raw Query packet payload into a readable SQL snippet.
// It strips leading metadata and keeps only a substring starting from the first
// recognizable SQL keyword.
func extractQuerySummary(chunk []byte, maxLen int) string {
	clean := summarizePrintable(chunk, maxLen*4)
	lower := strings.ToLower(clean)
	keywords := []string{"select", "insert", "create", "drop", "alter", "optimize", "show", "desc", "describe", "explain", "truncate"}

	idx := len(clean)
	for _, kw := range keywords {
		if pos := strings.Index(lower, kw); pos >= 0 && pos < idx {
			idx = pos
		}
	}

	if idx < len(clean) {
		clean = clean[idx:]
	} else {
		// Fallback: match USE if no other keyword found.
		if loc := regexp.MustCompile(`(?i)\buse\b`).FindStringIndex(clean); len(loc) == 2 {
			clean = clean[loc[0]:]
		}
	}
	if len(clean) > maxLen {
		clean = clean[:maxLen]
	}
	return strings.TrimSpace(clean)
}
