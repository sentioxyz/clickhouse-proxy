package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	log "sentioxyz/sentio-core/common/log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// Known client -> server packet types in ClickHouse native protocol.
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

// Known server -> client packet types in ClickHouse native protocol.
var serverPacketNames = map[uint64]string{
	0:  "Hello",
	1:  "Data",
	2:  "Exception",
	3:  "Progress",
	4:  "Pong",
	5:  "EndOfStream",
	6:  "ProfileInfo",
	7:  "Totals",
	8:  "Extremes",
	9:  "TablesStatusResponse",
	10: "Log",
	11: "TableColumns",
	12: "PartUUIDs",
	13: "ReadTaskRequest",
	14: "ProfileEvents",
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
	observer  *MetricsObserver
}

func newProxy(cfg Config, v Validator) *proxy {
	if v == nil {
		v = NoopValidator{}
	}
	return &proxy{
		cfg:       cfg,
		stats:     newPacketStats(),
		validator: v,
		observer:  NewMetricsObserver(),
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

// ParsedQuery holds the parsed SQL body and settings from a ClickHouse Query packet.
type ParsedQuery struct {
	SQL      string
	Settings map[string]string
}

// queryParser incrementally decodes Hello/Query packets to extract accurate SQL bodies and settings.
// It assumes the buffer starts on a packet boundary; for unknown packet types it drops the buffer.
type queryParser struct {
	version      int
	buf          []byte
	addendumDone bool
	disabled     bool // if true, stop parsing forever on this connection
}

// maxParserBufSize limits the parser buffer to prevent memory exhaustion.
const maxParserBufSize = 1 << 20 // 1MB

// resetBuf releases the underlying buffer memory to GC.
func (p *queryParser) resetBuf() {
	p.buf = nil
}

// consumeBuf removes the first n bytes from the buffer.
func (p *queryParser) consumeBuf(n int) {
	if n >= len(p.buf) {
		p.buf = nil
		return
	}
	remaining := make([]byte, len(p.buf)-n)
	copy(remaining, p.buf[n:])
	p.buf = remaining
}

// skipAddendum attempts to consume the optional "addendum" section.
func (p *queryParser) skipAddendum() (consumed int, ok bool, err error) {
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

func decodeQueryBody(data []byte, version int, forceSettings bool) (string, int, error) {
	cr := &countingReader{r: bytes.NewReader(data)}
	r := proto.NewReader(cr)

	// QueryID
	if _, err := r.Str(); err != nil {
		return "", cr.n, err
	}

	if proto.FeatureClientWriteInfo.In(version) {
		var info proto.ClientInfo
		if err := info.DecodeAware(r, version); err != nil {
			return "", cr.n, err
		}
	}

	if !proto.FeatureSettingsSerializedAsStrings.In(version) && !forceSettings {
		return "", cr.n, errors.New("settings not serialized as strings")
	}

	for {
		var s proto.Setting
		if err := s.Decode(r); err != nil {
			return "", cr.n, err
		}
		if s.Key == "" {
			break
		}
	}

	if proto.FeatureInterserverExternallyGrantedRoles.In(version) {
		if _, err := r.Str(); err != nil {
			return "", cr.n, err
		}
	}

	if proto.FeatureInterServerSecret.In(version) {
		if _, err := r.Str(); err != nil {
			return "", cr.n, err
		}
	}

	if _, err := r.UVarInt(); err != nil { // Stage
		return "", cr.n, err
	}
	if _, err := r.UVarInt(); err != nil { // Compression
		return "", cr.n, err
	}

	body, err := r.Str()
	if err != nil {
		return "", cr.n, err
	}

	if proto.FeatureParameters.In(version) {
		for {
			var p proto.Parameter
			if err := p.Decode(r); err != nil {
				return "", cr.n, err
			}
			if p.Key == "" {
				break
			}
		}
	}

	return body, cr.n, nil
}

func (p *queryParser) feed(chunk []byte) ([]ParsedQuery, error) {
	if p.disabled {
		return nil, nil
	}

	p.buf = append(p.buf, chunk...)

	if len(p.buf) > maxParserBufSize {
		p.resetBuf()
		p.disabled = true
		return nil, errors.New("parser buffer exceeded max size, discarding, parser disabled")
	}

	var out []ParsedQuery
	var decodeErr error
	for {
		if p.version != 0 && !p.addendumDone && proto.FeatureAddendum.In(p.version) {
			consumed, ok, err := p.skipAddendum()
			if err != nil {
				decodeErr = err
				p.resetBuf()
				p.disabled = true
				return out, decodeErr
			}
			if !ok {
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
			p.version = hello.ProtocolVersion
			consumed := n + cr.n
			p.consumeBuf(consumed)
		case 1: // Query
			if p.version == 0 {
				p.resetBuf()
				p.disabled = true
				return out, decodeErr
			}
			cr := &countingReader{r: bytes.NewReader(p.buf[n:])}
			r := proto.NewReader(cr)
			var q proto.Query
			if err := q.DecodeAware(r, p.version); err != nil {
				if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
					return out, decodeErr
				}
				body, consumed, derr := decodeQueryBody(p.buf[n:], p.version, true)
				if derr == nil {
					out = append(out, ParsedQuery{SQL: body, Settings: nil})
					p.consumeBuf(n + consumed)
					continue
				}
				decodeErr = err
				p.resetBuf()
				p.disabled = true
				return out, decodeErr
			}
			// Extract settings from proto.Query
			settings := make(map[string]string)
			for _, s := range q.Settings {
				settings[s.Key] = s.Value
			}
			out = append(out, ParsedQuery{SQL: q.Body, Settings: settings})
			consumed := n + cr.n
			p.consumeBuf(consumed)
		case 3, 4: // Cancel or Ping
			// These packets have no body, just consume the type byte.
			p.consumeBuf(n)
		default:
			// Unknown packet type (e.g., Data); reset to release memory.
			p.resetBuf()
			p.disabled = true
			return out, decodeErr
		}
	}
}

// detectPacketType tries to read a ClickHouse packet type from the beginning.
func detectPacketType(chunk []byte) string {
	if len(chunk) == 0 {
		return "unknown"
	}

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
	if typ < 32 {
		return fmt.Sprintf("type_%d", typ)
	}
	return "unknown"
}

// detectServerPacketType tries to read a ClickHouse server packet type.
func detectServerPacketType(chunk []byte) string {
	if len(chunk) == 0 {
		return "unknown"
	}
	if chunk[0]&0x80 != 0 {
		return "unknown"
	}

	typ, n := binary.Uvarint(chunk)
	if n <= 0 {
		return "unknown"
	}
	if name, ok := serverPacketNames[typ]; ok {
		return name
	}
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

	// Start background health check
	go p.runHealthCheck(ctx)

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

func (p *proxy) runHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	check := func() {
		d := net.Dialer{Timeout: 1 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", p.cfg.Upstream)
		if err != nil {
			p.observer.SetUpstreamHealth(false)
		} else {
			p.observer.SetUpstreamHealth(true)
			conn.Close()
		}
	}
	// Initial check
	check()
	for {
		select {
		case <-ticker.C:
			check()
		case <-ctx.Done():
			return
		}
	}
}

func (p *proxy) handleConnection(ctx context.Context, id int64, clientConn net.Conn) {
	p.observer.ConnectionOpened()
	defer p.observer.ConnectionClosed()
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
		p.observer.Error("dial", err)
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
			chunk := buf[:n]
			p.observer.BytesTransferred("upstream_to_client", float64(n))

			// Detect server packet types (e.g. Exception, EndOfStream, Data)
			pkt := detectServerPacketType(chunk)
			if pkt != "unknown" {
				p.observer.ServerPacket(pkt)
			}

			if p.cfg.IdleTimeout.Duration > 0 {
				_ = clientConn.SetWriteDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
			}
			if _, werr := clientConn.Write(chunk); werr != nil {
				log.Infof("[conn %d] upstream->client write error: %v", id, werr)
				p.observer.Error("client_write", werr)
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !isTimeout(err) && !errors.Is(err, net.ErrClosed) {
				log.Infof("[conn %d] upstream->client read error: %v", id, err)
			}
			if !errors.Is(err, io.EOF) {
				p.observer.Error("upstream_read", err)
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
			chunk := buf[:n]
			pkt := detectPacketType(chunk)
			p.stats.inc(pkt)
			p.observer.ClientPacket(pkt)
			p.observer.BytesTransferred("client_to_upstream", float64(n))

			// Feed all chunks to parser to capture Hello + Query accurately.
			// Parsed SQL will be validated through the Validator.
			sqls, perr := parser.feed(chunk)
			if perr != nil {
				log.Infof("[conn %d] query decode warning: %v", id, perr)
			}
			for _, parsed := range sqls {
				meta := QueryMeta{
					ConnID:       id,
					ClientAddr:   clientConn.RemoteAddr().String(),
					UpstreamAddr: p.cfg.Upstream,
					QueryPreview: parsed.SQL,
					Raw:          append([]byte(nil), chunk...),
					SQL:          parsed.SQL,
					Settings:     parsed.Settings,
				}
				if err := p.validator.ValidateQuery(ctx, meta); err != nil {
					log.Infof("[conn %d] query rejected: %v", id, err)
					return
				}
				if p.cfg.LogQueries {
					log.Infof("[conn %d %s -> %s] Query: [%s]", id, clientConn.RemoteAddr(), p.cfg.Upstream, parsed.SQL)
					log.Infof("[conn %d %s -> %s] Query raw hex: % X", id, clientConn.RemoteAddr(), p.cfg.Upstream, []byte(parsed.SQL))
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
				p.observer.Error("upstream_write", err)
				return
			}
			// If we successfully wrote a Query packet, increment the forwarded metric
			if pkt == "Query" {
				p.observer.QueryForwarded()
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && !isTimeout(readErr) && !errors.Is(readErr, net.ErrClosed) {
				log.Infof("[conn %d] client->upstream read error: %v", id, readErr)
			}
			if !errors.Is(readErr, io.EOF) {
				p.observer.Error("client_read", readErr)
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

// summarizePrintable extracts a compact ASCII summary from raw bytes.
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
		if loc := regexp.MustCompile(`(?i)\buse\b`).FindStringIndex(clean); len(loc) == 2 {
			clean = clean[loc[0]:]
		}
	}
	if len(clean) > maxLen {
		clean = clean[:maxLen]
	}
	return strings.TrimSpace(clean)
}
