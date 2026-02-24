package main

import (
	"bufio"
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
	0:  "Hello",
	1:  "Query",
	2:  "Data",
	3:  "Cancel",
	4:  "Ping",
	5:  "TablesStatusRequest",
	6:  "KeepAlive",
	7:  "Scalar",
	8:  "IgnoredPartUUIDs",
	9:  "ReadTaskResponse",
	10: "MergeTreeReadTaskResponse",
	11: "QueryPlan",
	// R7-1: type 12 is not used by ClickHouse TCPHandler (reserved), type 13 is ClusterFunctionReadTaskResponse
	13: "ClusterFunctionReadTaskResponse",
}

// Pre-compiled regex to avoid recompilation on every call
var useRegexp = regexp.MustCompile(`(?i)\buse\b`)

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
	15: "MergeTreeReadTaskRequest",
	16: "MergeTreeAllRangesAnnouncement",
	17: "TimezoneUpdate",
}

// Build reverse lookup for known packet names to avoid double-printing.
var packetNamesByName = func() map[string]struct{} {
	m := make(map[string]struct{}, len(packetNames))
	for _, v := range packetNames {
		m[v] = struct{}{}
	}
	return m
}()

// Client packet type constants not defined in ch-go v1.0.1
const (
	clientCodeKeepAlive                 proto.ClientCode = 6
	clientCodeScalar                    proto.ClientCode = 7
	clientCodeIgnoredPartUUIDs          proto.ClientCode = 8
	clientCodeReadTaskResponse          proto.ClientCode = 9
	clientCodeMergeTreeReadTaskResponse proto.ClientCode = 10
	clientCodeQueryPlan                 proto.ClientCode = 11
	// R1-5: ClusterFunctionReadTaskResponse (type 13) exists in ClickHouse TCPHandler
	clientCodeClusterFunctionReadTaskResponse proto.ClientCode = 13
)

// Protocol and buffer related constants
const (
	// fallbackRevision is the fallback protocol revision used when handshake decoding fails
	fallbackRevision = 54423
	// uuidSize is the fixed byte size of a ClickHouse UUID
	uuidSize = 16
	// defaultStreamingBufSize is the default bufio.Reader buffer size in streaming mode (128KB)
	defaultStreamingBufSize = 131072
)

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
	rewriter  Rewriter
	observer  *MetricsObserver
	// compressedBufPool removed: proto.Reader.ReadRaw always returns a newly allocated slice,
	// so data cannot be read into a pool-obtained buffer, making the pool ineffective.
	bufferPool sync.Pool // Reuse proto.Buffer to reduce allocations in the packet loop
}

func newProxy(cfg Config, v Validator, r Rewriter) *proxy {
	if v == nil {
		v = NoopValidator{}
	}
	if r == nil {
		r = NoopRewriter{}
	}
	return &proxy{
		cfg:       cfg,
		stats:     newPacketStats(),
		validator: v,
		rewriter:  r,
		observer:  NewMetricsObserver(),
	}
}

// getBuffer retrieves a proto.Buffer from bufferPool and resets its content.
func (p *proxy) getBuffer() *proto.Buffer {
	if v := p.bufferPool.Get(); v != nil {
		b := v.(*proto.Buffer)
		b.Reset()
		return b
	}
	return &proto.Buffer{}
}

// putBuffer returns a proto.Buffer to bufferPool. Buffers exceeding 1MB are discarded to prevent accumulation.
func (p *proxy) putBuffer(b *proto.Buffer) {
	const maxPoolBufSize = 1 * 1024 * 1024
	if len(b.Buf) <= maxPoolBufSize {
		p.bufferPool.Put(b)
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
// R1-13: Use in-place move instead of new allocation to reduce GC pressure
func (p *queryParser) consumeBuf(n int) {
	if n >= len(p.buf) {
		p.buf = nil
		return
	}
	copy(p.buf, p.buf[n:])
	p.buf = p.buf[:len(p.buf)-n]
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
				// On Hello decode failure, don't disable parser; use the default version and continue
				// This allows Query packets from higher-version clients to still be parsed
				log.Warnf("Hello decode failed (likely newer protocol): %v, using fallback version", err)
				p.version = fallbackRevision
				p.addendumDone = true
				p.resetBuf() // Clear buffer, skip the current Hello packet
				return out, nil
			}
			p.version = hello.ProtocolVersion
			consumed := n + cr.n
			p.consumeBuf(consumed)
		case 1: // Query
			if p.version == 0 {
				// version=0 means Hello was not parsed; attempt fallback with a generic version
				log.Infof("Query received with version=0, attempting fallback decode")
				p.version = fallbackRevision
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
				// R5-5: Both decode methods failed; include both errors to aid debugging
				log.Infof("Query decode failed: primary=%v, fallback=%v", err, derr)
				p.resetBuf()
				p.disabled = true
				return out, fmt.Errorf("query decode: primary: %v; fallback: %w", err, derr)
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
			// R3-6: Cancel(3) and Ping(4) are zero-payload packets (consistent with TCPHandler).
			// n is the encoded byte count of the UVarInt type code itself (usually 1 byte).
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

// R1-16: Add graceful shutdown draining mechanism
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

	// R1-16: Use WaitGroup to track in-flight connections for graceful shutdown
	var connWg sync.WaitGroup

	var connID int64
	for {
		clientConn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				// Wait for in-flight connections to complete (with timeout)
				shutdownTimeout := p.cfg.ShutdownTimeout.Duration
				if shutdownTimeout <= 0 {
					shutdownTimeout = 30 * time.Second
				}
				log.Infof("shutting down, waiting up to %s for in-flight connections...", shutdownTimeout)
				drainDone := make(chan struct{})
				go func() {
					connWg.Wait()
					close(drainDone)
				}()
				select {
				case <-drainDone:
					log.Infof("all in-flight connections drained")
				case <-time.After(shutdownTimeout):
					log.Infof("shutdown timeout exceeded, forcing close")
				}
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

		connWg.Add(1)
		go func() {
			defer connWg.Done()
			p.handleConnection(ctx, id, clientConn)
		}()
	}
}

func (p *proxy) runStatsPrinter(ctx context.Context) {
	// R7-2: Prevent panic from crashing the entire process (consistent with R6-3)
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("runStatsPrinter panic recovered: %v", r)
		}
	}()
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
	// R6-3: Prevent panic from crashing the entire process
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("runHealthCheck panic recovered: %v", r)
		}
	}()
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
		tc.SetNoDelay(true) // P2-12: Disable Nagle algorithm to reduce small packet latency
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
		tc.SetNoDelay(true) // P2-12: Disable Nagle algorithm to reduce small packet latency
	}

	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			clientConn.Close()
			upstreamConn.Close()
		})
	}

	// P2-6: Connection-level maximum lifetime limit
	if p.cfg.MaxConnectionLifetime.Duration > 0 {
		lifetimeTimer := time.AfterFunc(p.cfg.MaxConnectionLifetime.Duration, func() {
			log.Infof("[conn %d] max connection lifetime (%s) exceeded, closing", id, p.cfg.MaxConnectionLifetime.Duration)
			closeBoth()
		})
		defer lifetimeTimer.Stop()
	}

	var wg sync.WaitGroup
	wg.Add(2)

	if p.rewriter != nil && p.cfg.RewriterEnabled {
		// Streaming mode: copyClientToUpstreamStreaming must first synchronously complete
		// Hello/ServerHello/Addendum handshake (reads ServerHello from upstream),
		// before starting copyUpstreamToClient (to avoid two goroutines reading upstream concurrently).
		handshakeDone := make(chan struct{})
		// Upstream bufio.Reader, set by copyClientToUpstreamStreaming
		var upstreamBr *bufio.Reader
		// queryDoneCh: upstream goroutine sends signal when EndOfStream(5)/Exception(2) is detected,
		// the packet loop receives non-blockingly via select to reset compression state.
		// Buffer size set to 8 to ensure multiple rapid consecutive EndOfStream signals are not lost.
		queryDoneCh := make(chan struct{}, 8)
		// Chunked negotiation results, set by copyClientToUpstreamStreaming during handshake
		var srvSendChunked, clientRecvChunked string

		go func() {
			defer wg.Done()
			// Wait for handshake to complete before starting upstream→client copy
			<-handshakeDone
			p.copyUpstreamToClientFromReader(id, clientConn, upstreamConn, upstreamBr, queryDoneCh, srvSendChunked, clientRecvChunked)
			closeBoth()
		}()

		go func() {
			defer wg.Done()
			p.copyClientToUpstreamStreaming(ctx, id, clientConn, upstreamConn, handshakeDone, &upstreamBr, queryDoneCh, &srvSendChunked, &clientRecvChunked)
			closeBoth()
		}()
	} else {
		// Non-streaming mode: both goroutines start concurrently
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
	}

	wg.Wait()
	log.Infof("[conn %d] closed", id)
}

func (p *proxy) copyUpstreamToClient(id int64, clientConn, upstreamConn net.Conn) {
	p.copyUpstreamToClientFromReader(id, clientConn, upstreamConn, nil, nil, "", "")
}

// copyUpstreamToClientFromReader reads data from upstream and forwards it to the client.
// If upstreamBr is not nil, reads from bufio.Reader (streaming mode, to prevent losing cached data after ServerHello).
func (p *proxy) copyUpstreamToClientFromReader(id int64, clientConn, upstreamConn net.Conn, upstreamBr *bufio.Reader, queryDoneCh chan struct{}, srvSendChunked, clientRecvChunked string) {
	// Wrap Reader/Writer according to chunked negotiation results
	// srvSendChunked: whether server sends to proxy using chunked (requires ChunkedReader for deframing)
	// clientRecvChunked: whether client expects chunked from proxy (requires ChunkedWriter for framing)
	srvChunkedEnabled := srvSendChunked == "chunked"
	clientChunkedEnabled := clientRecvChunked == "chunked"
	var reader io.Reader
	if upstreamBr != nil {
		reader = NewChunkedReader(upstreamBr, srvChunkedEnabled)
	}
	var writer io.Writer = clientConn
	if clientChunkedEnabled {
		writer = NewChunkedWriter(clientConn, true)
	}
	if srvChunkedEnabled || clientChunkedEnabled {
		log.Infof("[conn %d] upstream→client chunked: srvSend=%v clientRecv=%v", id, srvChunkedEnabled, clientChunkedEnabled)
	}
	buf := make([]byte, 64*1024)
	for {
		if p.cfg.IdleTimeout.Duration > 0 {
			_ = upstreamConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}
		var n int
		var err error
		if reader != nil {
			n, err = reader.Read(buf)
		} else {
			n, err = upstreamConn.Read(buf)
		}
		if n > 0 {
			chunk := buf[:n]
			p.observer.BytesTransferred("upstream_to_client", float64(n))

			// Detect server packet types (e.g. Exception, EndOfStream, Data)
			// R2-6: In chunked mode, ChunkedReader may return data spanning packet boundaries,
			// chunk[0] may not be the packet type byte. Detection is best-effort.
			// EndOfStream(5) and Exception(2) are usually short packets occupying a single chunk,
			// so detection works in most cases but may occasionally misidentify.
			// TODO(R2-6): For high reliability, use structured packet boundary parsing in chunked mode.
			pkt := detectServerPacketType(chunk)
			if pkt != "unknown" {
				p.observer.ServerPacket(pkt)
			}

			// Detect upstream EndOfStream(5)/Exception(2) and notify packet loop to reset compression state
			// Aligns with ClickHouse client Connection::receivePacket behavior
			// Use buffered channel with non-blocking send to avoid signal loss
			if queryDoneCh != nil && (pkt == "EndOfStream" || pkt == "Exception") {
				select {
				case queryDoneCh <- struct{}{}:
				default:
					// Skip when channel is full (should not happen since buffer=8)
					log.Warnf("[conn %d] queryDoneCh full, signal dropped", id)
				}
			}

			if p.cfg.IdleTimeout.Duration > 0 {
				_ = clientConn.SetWriteDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
			}
			if _, werr := writer.Write(chunk); werr != nil {
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
				log.Infof("[conn %d] Processing parsed packet. SQL: %q, Settings: %v", id, parsed.SQL, parsed.Settings)
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

			// R1-3: Raw Patching: Strip/Replace Authentication Tokens
			// AFTER validation, BEFORE forwarding.
			// Security fix: besides replacing the key name, the token value must also be sanitized
			// Use eraseTokenValue to replace the key with promql_table and erase the value content
			chunk = eraseTokenValue(chunk, "x_auth_token")
			chunk = eraseTokenValue(chunk, "SQL_x_auth_token")

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

// copyClientToUpstreamStreaming uses the ch-go official protocol library for streaming parsing and SQL rewriting.
// Precisely decodes Hello/Query packets, completely eliminating risks of raw byte scanning packet splitting and false matches.
// handleDataBlock handles both compressed and uncompressed Data/Scalar blocks uniformly.
// Compressed mode: raw passthrough — read compressed frame header to determine size, forward raw bytes
// Uncompressed mode: directly decode BlockInfo + RawBlock → encode
func (p *proxy) handleDataBlock(
	ctx context.Context,
	id int64,
	code proto.ClientCode,
	chReader *proto.Reader,
	br *bufio.Reader,
	upstreamWriter io.Writer,
	queryCompression proto.Compression,
	revision int,
) error {
	// Read block_name (uncompressed, always in the plaintext stream)
	blockName, err := chReader.Str()
	if err != nil {
		return fmt.Errorf("block name: %w", err)
	}

	// P3-4: Use bufferPool to reduce header encoding memory allocations (Data blocks are the most frequent path)
	hdrBuf := p.getBuffer()
	hdrBuf.PutByte(byte(code))
	hdrBuf.PutString(blockName)

	// Check if context has been cancelled
	if err := ctx.Err(); err != nil {
		p.putBuffer(hdrBuf)
		return fmt.Errorf("context cancelled: %w", err)
	}

	// Write header first (packet_code + block_name)
	if _, err := upstreamWriter.Write(hdrBuf.Buf); err != nil {
		p.putBuffer(hdrBuf)
		return fmt.Errorf("write block header: %w", err)
	}
	p.putBuffer(hdrBuf)

	if queryCompression == proto.CompressionEnabled {
		p.observer.StreamingDataBlock("compressed")
		// ========== Compressed mode: streaming frame-by-frame raw passthrough (multi-frame support) ==========
		// ClickHouse compressed frame format:
		//   [16 bytes: CityHash128 checksum]
		//   [1 byte: compression method (0x82=LZ4, 0x90=ZSTD, 0x02=none)]
		//   [4 bytes LE: compressed_size (includes 9-byte sub-header + compressed data)]
		//   [4 bytes LE: decompressed_size]
		//   [N bytes: compressed data, where N = compressed_size - 9]
		// Total frame size = 16 (checksum) + compressed_size
		//
		// Important: a logical Data Block may consist of multiple compressed frames.
		// When the Block size exceeds DBMS_MAX_COMPRESSED_BLOCK_SIZE (default 1MB),
		// the ClickHouse client's CompressedWriteBuffer splits the data into multiple consecutive compressed frames.
		// We loop-read all consecutive compressed frames from the stream, detecting boundaries by the next frame's method byte.

		const frameHeaderSize = 16 + 1 + 4 + 4 // = 25 bytes
		const maxCompressedFrameSize = 32 * 1024 * 1024
		const maxDecompressedSize = 256 * 1024 * 1024
		totalFrameBytes := 0

		for {
			// Use chReader.ReadRaw to ensure reading from the same buffer layer
			header, err := chReader.ReadRaw(frameHeaderSize)
			if err != nil {
				return fmt.Errorf("compressed frame header: %w", err)
			}

			// Extract compressed_size from header[17:21] (little-endian uint32)
			compressedSize := binary.LittleEndian.Uint32(header[17:21])

			// Sanity check: compressed_size must be >= 9 (sub-header size)
			if compressedSize < 9 {
				return fmt.Errorf("invalid compressed_size %d (< 9)", compressedSize)
			}
			if compressedSize > maxCompressedFrameSize {
				return fmt.Errorf("compressed_size %d exceeds limit %d", compressedSize, maxCompressedFrameSize)
			}

			// Remaining compressed data bytes = compressed_size - 9 (sub-header already read)
			remainingDataSize := int(compressedSize) - 9

			compressedData, err := chReader.ReadRaw(remainingDataSize)
			if err != nil {
				return fmt.Errorf("compressed frame data: %w", err)
			}

			// R6-1: Use bufferPool to reduce compressed frame memory allocations in high-frequency INSERT scenarios
			fBuf := p.getBuffer()
			fBuf.Buf = append(fBuf.Buf[:0], header...)
			fBuf.Buf = append(fBuf.Buf, compressedData...)
			if _, err := upstreamWriter.Write(fBuf.Buf); err != nil {
				p.putBuffer(fBuf)
				return fmt.Errorf("write compressed frame: %w", err)
			}
			p.putBuffer(fBuf)
			totalFrameBytes += frameHeaderSize + remainingDataSize

			// R1-4: Detect if there are subsequent compressed frames
			// Fix: use chReader's underlying bufio.Reader for peek, ensuring consistency with ReadRaw's buffer layer.
			// In chunked mode, br may be a new bufio.Reader on top of ChunkedReader,
			// and chReader is also on the same bufio.Reader, so they are consistent.
			// Note: chReader.ReadRaw ultimately reads from br.Read, so br.Peek is consistent with it.
			nextBytes, peekErr := br.Peek(frameHeaderSize) // 25 bytes
			if peekErr != nil || len(nextBytes) < frameHeaderSize {
				// Cannot peek complete frame header; current frame is the last one
				break
			}
			methodByte := nextBytes[16]
			if methodByte != 0x82 && methodByte != 0x90 && methodByte != 0x02 {
				// Not a compression method byte; the next one is not a compressed frame
				break
			}
			// Additional validation: compressed_size >= 9 and decompressed_size > 0 and both within reasonable range
			peekCompSize := binary.LittleEndian.Uint32(nextBytes[17:21])
			peekDecompSize := binary.LittleEndian.Uint32(nextBytes[21:25])
			if peekCompSize < 9 || peekCompSize > maxCompressedFrameSize || peekDecompSize == 0 || peekDecompSize > maxDecompressedSize {
				// Does not meet reasonable compressed frame parameter range; treated as non-compressed frame
				break
			}
			// Continue reading the next compressed frame
		}

		if p.cfg.LogQueries {
			log.Infof("[conn %d] streaming: forwarded compressed %s block (streaming passthrough, %d frame bytes)",
				id, code, totalFrameBytes)
		}
	} else {
		p.observer.StreamingDataBlock("uncompressed")
		// ========== Uncompressed mode ==========
		// Need to decode BlockInfo and RawBlock to determine block boundaries.
		blockInfo, err := decodeBlockInfoCompat(chReader)
		if err != nil {
			return fmt.Errorf("BlockInfo decode: %w", err)
		}

		// P3-5: Use bufferPool to reduce encoding buffer memory allocations
		encBuf := p.getBuffer()
		encodeBlockInfoCompat(encBuf, blockInfo)

		// P2-1: Empty Block fast path — skip full DecodeRawBlock/EncodeRawBlock
		// ClickHouse sends an empty Block (columns=0, rows=0) as an end marker when a Query finishes.
		// Empty Blocks account for ~50% of all Data packets; skipping full column parsing+encoding significantly reduces overhead.
		// Detection method: Peek the first 2 bytes; if both num_columns and num_rows UVarInts are 0,
		// then column data does not need to be decoded.
		peekBytes, peekErr := br.Peek(2)
		if peekErr == nil && len(peekBytes) >= 2 && peekBytes[0] == 0 && peekBytes[1] == 0 {
			// Empty Block fast path: directly consume 2 bytes (num_columns=0, num_rows=0)
			br.Discard(2)
			encBuf.PutUVarInt(0)
			encBuf.PutUVarInt(0)
			if _, err := upstreamWriter.Write(encBuf.Buf); err != nil {
				p.putBuffer(encBuf)
				return fmt.Errorf("write empty block: %w", err)
			}
			p.putBuffer(encBuf)
			if p.cfg.LogQueries {
				log.Infof("[conn %d] streaming: forwarded empty %s block (fast path, %d bytes)",
					id, code, len(encBuf.Buf))
			}
		} else {
			// Non-empty Block: full decode-encode
			var block proto.Block
			var results proto.Results
			if err := block.DecodeRawBlock(chReader, revision, results.Auto()); err != nil {
				p.putBuffer(encBuf)
				return fmt.Errorf("block raw decode: %w", err)
			}

			if block.End() {
				// columns=0, rows=0 case (theoretically intercepted by fast path; this is defensive handling)
				encBuf.PutUVarInt(0)
				encBuf.PutUVarInt(0)
			} else {
				inputCols, err := resultsToInput(results)
				if err != nil {
					p.putBuffer(encBuf)
					return fmt.Errorf("resultsToInput: %w", err)
				}
				if err := block.EncodeRawBlock(encBuf, revision, inputCols); err != nil {
					p.putBuffer(encBuf)
					return fmt.Errorf("block encode: %w", err)
				}
			}

			if _, err := upstreamWriter.Write(encBuf.Buf); err != nil {
				p.putBuffer(encBuf)
				return fmt.Errorf("write uncompressed block: %w", err)
			}

			if p.cfg.LogQueries {
				log.Infof("[conn %d] streaming: forwarded %s block (%d cols, %d rows, %d bytes)",
					id, code, block.Columns, block.Rows, len(encBuf.Buf))
			}
			p.putBuffer(encBuf)
		}
	}

	return nil
}

func (p *proxy) copyClientToUpstreamStreaming(ctx context.Context, id int64, clientConn, upstreamConn net.Conn, handshakeDone chan struct{}, upstreamBrOut **bufio.Reader, queryDoneCh chan struct{}, srvSendChunkedOut, clientRecvChunkedOut *string) {
	// Ensure handshakeDone is closed when function exits, preventing copyUpstreamToClient goroutine from blocking forever
	handshakeClosed := false
	defer func() {
		if !handshakeClosed {
			close(handshakeDone)
		}
	}()

	bufSize := p.cfg.StreamingBufSize
	if bufSize <= 0 {
		bufSize = defaultStreamingBufSize
	}

	br := bufio.NewReaderSize(clientConn, bufSize)
	chReader := proto.NewReader(br)

	// Create upstream bufio.Reader (copyUpstreamToClient will also read from this)
	upBr := bufio.NewReaderSize(upstreamConn, bufSize)
	*upstreamBrOut = upBr

	// ========== Phase 1: Hello Handshake ==========
	if p.cfg.IdleTimeout.Duration > 0 {
		_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
	}
	typeByte, err := br.ReadByte()
	if err != nil {
		log.Warnf("[conn %d] streaming: read hello type error: %v", id, err)
		return
	}
	if typeByte != byte(proto.ClientCodeHello) {
		log.Infof("[conn %d] streaming: expected Hello(0), got %d, falling back", id, typeByte)
		_ = br.UnreadByte()
		close(handshakeDone)
		handshakeClosed = true
		p.fallbackRawCopy(id, br, clientConn, upstreamConn)
		return
	}

	// P1-3: Use TeeReader to record Hello's raw bytes for raw passthrough
	// Avoids the risk of ClientHello.Encode() potentially omitting new protocol version fields
	var helloBuf bytes.Buffer
	teeReader := io.TeeReader(br, &helloBuf)
	teeBr := bufio.NewReaderSize(teeReader, bufSize)
	teeChReader := proto.NewReader(teeBr)

	var hello proto.ClientHello
	if err := hello.Decode(teeChReader); err != nil {
		log.Warnf("[conn %d] streaming: Hello decode error: %v", id, err)
		return
	}
	clientRevision := hello.ProtocolVersion
	log.Infof("[conn %d] streaming: Hello decoded, client=%s revision=%d", id, hello.Name, clientRevision)

	// Record handshake start time
	handshakeStart := time.Now()

	// Send Hello type byte + original decoded bytes together to upstream
	// helloBuf contains all Hello raw bytes recorded by TeeReader (excluding typeByte)
	helloPayload := make([]byte, 1+helloBuf.Len())
	helloPayload[0] = typeByte
	copy(helloPayload[1:], helloBuf.Bytes())
	if _, err := upstreamConn.Write(helloPayload); err != nil {
		log.Infof("[conn %d] streaming: write hello error: %v", id, err)
		return
	}

	// ========== Phase 1.5: Synchronously read and forward ServerHello ==========
	// The ClickHouse client calls receiveHello() after sendHello(),
	// and only calls sendAddendum() after receiving ServerHello.
	// Therefore the proxy must first obtain ServerHello and forward it to client,
	// before the client will send Addendum.
	//
	// ServerHello format (contains different fields based on client_tcp_protocol_version):
	// [packet_type: UVarInt=0] [name: String] [major: UVarInt] [minor: UVarInt]
	// [revision: UVarInt]
	// [parallel_replicas_version: UVarInt]  (>= 54471)
	// [timezone: String]                    (>= 54058)
	// [display_name: String]                (>= 54372)
	// [version_patch: UVarInt]              (>= 54401)
	// [proto_send_chunked_srv: String]      (>= 54470)
	// [proto_recv_chunked_srv: String]      (>= 54470)
	// [password_rules: UVarInt(count) + pairs of Strings]  (>= password rule version)
	// [nonce: Int64]                        (>= interserver_secret_v2)
	// [settings: Settings]                  (>= server_settings version)
	// [query_plan_serialization_version: UVarInt] (>= query_plan version)
	if p.cfg.IdleTimeout.Duration > 0 {
		_ = upstreamConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
	}

	// P0-1 Fix: Use TeeReader to record all raw bytes of ServerHello
	// Benefit: regardless of what trailing fields ClickHouse adds in the future (password_rules, nonce, settings,
	// query_plan_serialization_version, etc.), they will be automatically recorded and transparently forwarded to the client.
	// Eliminates the risk of data loss in packet-splitting scenarios with the old approach's blind Buffered() fetch.
	var serverHelloRaw bytes.Buffer
	teeUpReader := io.TeeReader(upBr, &serverHelloRaw)
	teeUpBr := bufio.NewReaderSize(teeUpReader, bufSize)
	teeUpChReader := proto.NewReader(teeUpBr)

	// packet_type
	pktType, err := teeUpChReader.UVarInt()
	if err != nil {
		log.Errorf("[conn %d] streaming: read ServerHello packet_type error: %v", id, err)
		return
	}
	// P1 #4: Check if it is ServerHello (type 0)
	// If upstream returns Exception (type 2), forward directly to client
	if pktType != 0 {
		log.Errorf("[conn %d] streaming: expected ServerHello (type 0), got type %d", id, pktType)
		// serverHelloRaw already contains the pktType byte
		// Then drain remaining buffered data in teeUpBr (error messages, etc.)
		if buffered := teeUpBr.Buffered(); buffered > 0 {
			drainBuf := make([]byte, buffered)
			teeUpBr.Read(drainBuf)
		}
		clientConn.Write(serverHelloRaw.Bytes())
		return
	}

	// name
	serverName, err := teeUpChReader.Str()
	if err != nil {
		log.Errorf("[conn %d] streaming: read ServerHello name error: %v", id, err)
		return
	}

	// major
	major, err := teeUpChReader.UVarInt()
	if err != nil {
		log.Errorf("[conn %d] streaming: read ServerHello major error: %v", id, err)
		return
	}

	// minor
	minor, err := teeUpChReader.UVarInt()
	if err != nil {
		log.Errorf("[conn %d] streaming: read ServerHello minor error: %v", id, err)
		return
	}

	// revision
	serverRevUint, err := teeUpChReader.UVarInt()
	if err != nil {
		log.Errorf("[conn %d] streaming: read ServerHello revision error: %v", id, err)
		return
	}
	serverRevision := int(serverRevUint)
	log.Infof("[conn %d] streaming: ServerHello: name=%s version=%d.%d revision=%d", id, serverName, major, minor, serverRevision)

	// The following fields are based on clientRevision (server sends conditionally based on client_tcp_protocol_version)
	if proto.FeatureVersionedParallelReplicas.In(clientRevision) {
		if _, err := teeUpChReader.UVarInt(); err != nil {
			log.Warnf("[conn %d] streaming: read ServerHello parallel_replicas_version error: %v", id, err)
			return
		}
	}
	if proto.FeatureTimezone.In(clientRevision) {
		if _, err := teeUpChReader.Str(); err != nil {
			log.Warnf("[conn %d] streaming: read ServerHello timezone error: %v", id, err)
			return
		}
	}
	if proto.FeatureDisplayName.In(clientRevision) {
		if _, err := teeUpChReader.Str(); err != nil {
			log.Warnf("[conn %d] streaming: read ServerHello display_name error: %v", id, err)
			return
		}
	}
	if proto.FeatureVersionPatch.In(clientRevision) {
		if _, err := teeUpChReader.UVarInt(); err != nil {
			log.Warnf("[conn %d] streaming: read ServerHello version_patch error: %v", id, err)
			return
		}
	}
	// chunked protocol negotiation (read server's chunked caps from ServerHello)
	var srvSendChunked, srvRecvChunked string
	if proto.FeatureChunkedPackets.In(clientRevision) {
		var err error
		srvSendChunked, err = teeUpChReader.Str()
		if err != nil {
			log.Warnf("[conn %d] streaming: read ServerHello proto_send_chunked error: %v", id, err)
			return
		}
		srvRecvChunked, err = teeUpChReader.Str()
		if err != nil {
			log.Warnf("[conn %d] streaming: read ServerHello proto_recv_chunked error: %v", id, err)
			return
		}
		log.Infof("[conn %d] streaming: ServerHello chunked: send=%q recv=%q", id, srvSendChunked, srvRecvChunked)
		// Save negotiation results for copyUpstreamToClientFromReader
		if srvSendChunkedOut != nil {
			*srvSendChunkedOut = srvSendChunked
		}
	}

	// P0-3 Fix: Completely drain remaining buffered data from teeUpBr into serverHelloRaw.
	// Use loop drain to ensure all data is read even if bufio returns data in batches.
	// This data consists of ServerHello trailing fields (password_rules, nonce, settings, etc.),
	// which have been prefetched by the underlying socket or bufio but not yet processed by teeReader.
	// Must be completed before close(handshakeDone), otherwise copyUpstreamToClientFromReader
	// would parse these non-chunked ServerHello trailing data with ChunkedReader after starting.
	for {
		buffered := teeUpBr.Buffered()
		if buffered <= 0 {
			break
		}
		drainBuf := make([]byte, buffered)
		n, err := teeUpBr.Read(drainBuf)
		if n > 0 {
			log.Infof("[conn %d] streaming: ServerHello tail drained: %d bytes", id, n)
		}
		if err != nil {
			break
		}
	}

	// Send the complete ServerHello raw bytes recorded by TeeReader to the client
	if _, err := clientConn.Write(serverHelloRaw.Bytes()); err != nil {
		log.Errorf("[conn %d] streaming: write ServerHello to client error: %v", id, err)
		return
	}
	log.Infof("[conn %d] streaming: ServerHello forwarded (%d bytes)", id, serverHelloRaw.Len())

	// Use min(clientRevision, serverRevision) as the effective negotiated revision
	revision := clientRevision
	if serverRevision > 0 && serverRevision < revision {
		revision = serverRevision
	}
	log.Infof("[conn %d] streaming: negotiated revision=%d (client=%d, server=%d)", id, revision, clientRevision, serverRevision)

	p.observer.HandshakeCompleted(time.Since(handshakeStart).Seconds())

	// Release handshake lock first to let copyUpstreamToClient start
	close(handshakeDone)
	handshakeClosed = true

	// ========== Phase 1.6: Handle Addendum ==========
	// Note: at this point copyUpstreamToClient has started, forwarding ServerHello's remaining fields to client.
	// The client sends Addendum after receiving the complete ServerHello.
	// Addendum fields are based on server_revision (client uses revision read from ServerHello):
	//   1. quota_key: String (server_revision >= FeatureQuotaKey=54458)
	//   2. proto_send_chunked: String (server_revision >= FeatureChunkedPackets=54470)
	//   3. proto_recv_chunked: String (server_revision >= FeatureChunkedPackets=54470)
	//   4. parallel_replicas_version: UVarInt (server_revision >= FeatureVersionedParallelReplicas=54471)
	// Use serverRevision as the condition (since client's sendAddendum is based on server_revision)
	addendumRevision := serverRevision
	var clientSendChunked, clientRecvChunked string
	// P1 #5: Double gating — client must also support Addendum to send it
	// Client's sendAddendum is based on server_revision, but the client code version may be too old
	// and may not know the Addendum protocol at all; in this case, we should not wait
	if proto.FeatureAddendum.In(addendumRevision) && proto.FeatureAddendum.In(clientRevision) {
		// Set timeout to wait for client to send Addendum
		if p.cfg.IdleTimeout.Duration > 0 {
			_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}

		// 1. quota_key
		quotaKey, err := chReader.Str()
		if err != nil {
			log.Infof("[conn %d] streaming: read addendum quota_key error: %v", id, err)
			return
		}

		abuf := &proto.Buffer{}
		abuf.PutString(quotaKey)

		// chunked 协议协商 — 同时检查客户端 revision，确保客户端实际会发送这些字段
		if proto.FeatureChunkedPackets.In(addendumRevision) && proto.FeatureChunkedPackets.In(clientRevision) {
			var err error
			clientSendChunked, err = chReader.Str()
			if err != nil {
				log.Warnf("[conn %d] streaming: read addendum proto_send_chunked error: %v", id, err)
				return
			}
			clientRecvChunked, err = chReader.Str()
			if err != nil {
				log.Warnf("[conn %d] streaming: read addendum proto_recv_chunked error: %v", id, err)
				return
			}
			log.Infof("[conn %d] streaming: client chunked: send=%q recv=%q",
				id, clientSendChunked, clientRecvChunked)

			// Pass through client's chunked capability to upstream
			abuf.PutString(clientSendChunked)
			abuf.PutString(clientRecvChunked)
			// Save negotiation results
			if clientRecvChunkedOut != nil {
				*clientRecvChunkedOut = clientRecvChunked
			}
		}

		// 4. parallel replicas protocol version — 同时检查客户端 revision
		if proto.FeatureVersionedParallelReplicas.In(addendumRevision) && proto.FeatureVersionedParallelReplicas.In(clientRevision) {
			parallelVersion, err := chReader.UVarInt()
			if err != nil {
				log.Infof("[conn %d] streaming: read addendum parallel_replicas_version error: %v", id, err)
				return
			}
			abuf.PutUVarInt(parallelVersion)
		}

		if _, err := upstreamConn.Write(abuf.Buf); err != nil {
			log.Infof("[conn %d] streaming: write addendum error: %v", id, err)
			return
		}
		log.Infof("[conn %d] streaming: addendum forwarded (quota_key=%q, clientSendChunked=%q, clientRecvChunked=%q)", id, quotaKey, clientSendChunked, clientRecvChunked)
	}

	// ========== Phase 2: Packet Loop ==========
	// Chunked adaptation layer (inserted after handshake, before packet loop starts)
	// Note: ClickHouse chunked protocol is only enabled after handshake; Hello/ServerHello/Addendum are never chunked
	clientChunkedEnabled := clientSendChunked == "chunked"
	upstreamChunkedEnabled := srvRecvChunked == "chunked"
	if clientChunkedEnabled || upstreamChunkedEnabled {
		log.Infof("[conn %d] streaming: enabling chunked adapters (client→proxy: %v, proxy→upstream: %v)",
			id, clientChunkedEnabled, upstreamChunkedEnabled)
	}
	if clientChunkedEnabled {
		// Client sends chunked data → wrap br with ChunkedReader
		// This way br.ReadByte() and chReader can both transparently read "raw" protocol data
		chunkedClientReader := NewChunkedReader(br, true)
		br = bufio.NewReaderSize(chunkedClientReader, bufSize)
		chReader = proto.NewReader(br)
	}
	var upstreamWriter io.Writer = upstreamConn
	if upstreamChunkedEnabled {
		// Upstream expects chunked data → wrap with ChunkedWriter
		upstreamWriter = NewChunkedWriter(upstreamConn, true)
	}
	_ = upstreamWriter // The following packet loop / handleDataBlock writes to upstream through this

	// Track the current Query's compression state to determine Data block handling
	var queryCompression proto.Compression

	// Two-phase packet loop state machine, aligned with ClickHouse TCPHandler's
	// receivePacketsExpectQuery / receivePacketsExpectData pattern.
	// expectQuery: waiting for new Query (accepts only Query/Ping/Cancel/TablesStatusRequest/IgnoredPartUUIDs)
	// expectData: Query in progress (accepts only Data/Scalar/Ping/Cancel/ReadTaskResponse etc.)
	const (
		expectQuery = 0
		expectData  = 1
	)
	expectState := expectQuery

	for {
		// Detect upstream EndOfStream/Exception via channel, reset compression state
		// Non-blocking select: process signal if available, otherwise continue
		if expectState == expectData && queryDoneCh != nil {
			select {
			case <-queryDoneCh:
				expectState = expectQuery
				queryCompression = proto.CompressionDisabled
			default:
			}
		}

		if p.cfg.IdleTimeout.Duration > 0 {
			_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}

		codeByte, err := br.ReadByte()
		if err != nil {
			if !errors.Is(err, io.EOF) && !isTimeout(err) {
				log.Infof("[conn %d] streaming: read packet type error: %v", id, err)
			}
			return
		}
		code := proto.ClientCode(codeByte)

		switch code {
		case proto.ClientCodeQuery:
			// Use custom Query decoder (supports all protocol versions, precisely aligned with ClickHouse TCPHandler)
			decodeStart := time.Now()
			eq, err := decodeQueryCustom(chReader, revision)
			if err != nil {
				log.Warnf("[conn %d] streaming: Query decode error: %v", id, err)
				return
			}
			p.observer.QueryDecoded(time.Since(decodeStart).Seconds())
			p.observer.ClientPacket("Query")
			p.stats.inc("Query")

			originalSQL := eq.Body

			// R4-2: Streaming mode must call Validator for signature verification
			// Non-streaming mode calls p.validator.ValidateQuery in copyClientToUpstream,
			// streaming mode previously lacked this step, causing JWS signature verification to be bypassed.
			if p.validator != nil {
				settingsMap := make(map[string]string, len(eq.Settings)+len(eq.OldSettings))
				for _, s := range eq.Settings {
					settingsMap[s.Key] = s.Value
				}
				for _, s := range eq.OldSettings {
					settingsMap[s.Key] = fmt.Sprintf("%d", s.Value)
				}
				meta := QueryMeta{
					ConnID:       id,
					ClientAddr:   clientConn.RemoteAddr().String(),
					UpstreamAddr: p.cfg.Upstream,
					SQL:          eq.Body,
					Settings:     settingsMap,
				}
				if err := p.validator.ValidateQuery(ctx, meta); err != nil {
					log.Infof("[conn %d] streaming: query rejected by validator: %v", id, err)
					return
				}
			}

			// P2-1: Strip auth token settings after signature verification; do not send to ClickHouse Server
			// In streaming mode, remove token after decode; it will naturally be excluded during encode
			eq.Settings = stripAuthTokenSettings(eq.Settings)
			eq.OldSettings = stripAuthTokenOldSettings(eq.OldSettings)

			// SQL rewriting
			if p.rewriter != nil && p.cfg.RewriterEnabled {
				rewriteStart := time.Now()
				rewrittenSQL, err := p.rewriter.Rewrite(ctx, eq.Body)
				p.observer.Rewritten(time.Since(rewriteStart).Seconds())
				if err != nil {
					log.Warnf("[conn %d] streaming: SQL rewrite failed: %v", id, err)
				} else if rewrittenSQL != eq.Body {
					log.Infof("[conn %d] streaming rewrite: %q -> %q", id, eq.Body, rewrittenSQL)
					eq.Body = rewrittenSQL
				}
			}

			if p.cfg.LogQueries {
				log.Infof("[conn %d] streaming Query: %q", id, originalSQL)
			}

			// Re-encode Query using custom encoder (strict mirror of decodeQueryCustom)
			qbuf := p.getBuffer()
			encodeQueryCustom(qbuf, eq, revision)
			if _, err := upstreamWriter.Write(qbuf.Buf); err != nil {
				p.putBuffer(qbuf)
				log.Infof("[conn %d] streaming: write query error: %v", id, err)
				return
			}
			p.putBuffer(qbuf)
			p.observer.QueryForwarded()

			// Save compression state, which affects subsequent Data block handling
			queryCompression = eq.Compression
			expectState = expectData // Align with TCPHandler: enter expectData phase after Query

		case proto.ClientCodeData:
			// Data 包在 expectQuery 状态下可能是竞态条件：
			// DDL 等快速查询的 EndOfStream 信号可能在客户端发送空 Data 块之前就到达，
			// 导致 queryDoneCh 将状态过早回退到 expectQuery。
			// 容忍此情况，将 Data 块正常透传给 upstream（ClickHouse 会自行判断是否合法）。
			if expectState == expectQuery {
				log.Warnf("[conn %d] streaming: Data packet in expectQuery state (possible race), forwarding anyway", id)
			}
			p.observer.ClientPacket("Data")
			p.stats.inc("Data")
			if err := p.handleDataBlock(ctx, id, proto.ClientCodeData, chReader, br, upstreamWriter, queryCompression, revision); err != nil {
				log.Infof("[conn %d] streaming: Data block error: %v", id, err)
				return
			}

		case clientCodeScalar:
			// R2-1: Scalar throws UNEXPECTED_PACKET exception in TCPHandler::receivePacketsExpectQuery
			if expectState == expectQuery {
				log.Errorf("[conn %d] streaming: unexpected Scalar packet in expectQuery state, closing (TCPHandler rejects)", id)
				return
			}
			p.observer.ClientPacket("Scalar")
			p.stats.inc("Scalar")
			if err := p.handleDataBlock(ctx, id, clientCodeScalar, chReader, br, upstreamWriter, queryCompression, revision); err != nil {
				log.Infof("[conn %d] streaming: Scalar block error: %v", id, err)
				return
			}

		case proto.ClientCodePing:
			p.observer.ClientPacket("Ping")
			p.stats.inc("Ping")
			pbuf := p.getBuffer()
			proto.ClientCodePing.Encode(pbuf)
			if _, err := upstreamWriter.Write(pbuf.Buf); err != nil {
				p.putBuffer(pbuf)
				return
			}
			p.putBuffer(pbuf)

		case proto.ClientCodeCancel:
			p.observer.ClientPacket("Cancel")
			p.stats.inc("Cancel")
			cbuf := p.getBuffer()
			proto.ClientCodeCancel.Encode(cbuf)
			if _, err := upstreamWriter.Write(cbuf.Buf); err != nil {
				p.putBuffer(cbuf)
				return
			}
			p.putBuffer(cbuf)
			// P1-4: Reset query state after Cancel
			// Aligned with ClickHouse TCPHandler::processCancel behavior:
			// Cancel means the client requests aborting the current query; old compression state should not be reused
			expectState = expectQuery
			queryCompression = proto.CompressionDisabled

		case clientCodeKeepAlive:
			// KeepAlive has no payload; just forward the type code
			p.observer.ClientPacket("KeepAlive")
			p.stats.inc("KeepAlive")
			kbuf := p.getBuffer()
			kbuf.PutByte(byte(clientCodeKeepAlive))
			if _, err := upstreamWriter.Write(kbuf.Buf); err != nil {
				p.putBuffer(kbuf)
				return
			}
			p.putBuffer(kbuf)

		case proto.ClientTablesStatusRequest:
			// R2-1: TablesStatusRequest throws UNEXPECTED_PACKET exception in TCPHandler::receivePacketsExpectData
			if expectState == expectData {
				log.Errorf("[conn %d] streaming: unexpected TablesStatusRequest in expectData state, closing (TCPHandler rejects)", id)
				return
			}
			p.observer.ClientPacket("TablesStatusRequest")
			p.stats.inc("TablesStatusRequest")
			// Structure: [num_tables: UVarInt] + [database: String, table: String] × N
			numTables, err := chReader.UVarInt()
			if err != nil {
				log.Infof("[conn %d] streaming: TablesStatusRequest decode error: %v", id, err)
				return
			}
			tbuf := p.getBuffer() // P2-10: Uniformly use bufferPool
			proto.ClientTablesStatusRequest.Encode(tbuf)
			tbuf.PutUVarInt(numTables)
			for i := uint64(0); i < numTables; i++ {
				db, err := chReader.Str()
				if err != nil {
					p.putBuffer(tbuf)
					log.Infof("[conn %d] streaming: TablesStatusRequest db read error: %v", id, err)
					return
				}
				tbl, err := chReader.Str()
				if err != nil {
					p.putBuffer(tbuf)
					log.Infof("[conn %d] streaming: TablesStatusRequest table read error: %v", id, err)
					return
				}
				tbuf.PutString(db)
				tbuf.PutString(tbl)
			}
			if _, err := upstreamWriter.Write(tbuf.Buf); err != nil {
				p.putBuffer(tbuf)
				return
			}
			p.putBuffer(tbuf)

		case clientCodeIgnoredPartUUIDs:
			// R2-1: IgnoredPartUUIDs throws UNEXPECTED_PACKET exception in TCPHandler::receivePacketsExpectData
			if expectState == expectData {
				log.Errorf("[conn %d] streaming: unexpected IgnoredPartUUIDs in expectData state, closing (TCPHandler rejects)", id)
				return
			}
			// IgnoredPartUUIDs: [count: UVarInt] + [UUID(16 bytes) × count]
			p.observer.ClientPacket("IgnoredPartUUIDs")
			p.stats.inc("IgnoredPartUUIDs")
			count, err := chReader.UVarInt()
			if err != nil {
				log.Infof("[conn %d] streaming: IgnoredPartUUIDs count error: %v", id, err)
				return
			}
			ibuf := p.getBuffer() // P2-10: Uniformly use bufferPool
			ibuf.PutByte(byte(clientCodeIgnoredPartUUIDs))
			ibuf.PutUVarInt(count)
			for i := uint64(0); i < count; i++ {
				uuid, err := chReader.ReadRaw(uuidSize)
				if err != nil {
					p.putBuffer(ibuf)
					log.Infof("[conn %d] streaming: IgnoredPartUUIDs uuid read error: %v", id, err)
					return
				}
				ibuf.Buf = append(ibuf.Buf, uuid...)
			}
			if _, err := upstreamWriter.Write(ibuf.Buf); err != nil {
				p.putBuffer(ibuf)
				return
			}
			p.putBuffer(ibuf)

		case clientCodeReadTaskResponse:
			// ReadTaskResponse: [response: String]
			// According to ClickHouse TCPHandler::receiveReadTaskResponseAssumeLocked,
			// there is only one readStringBinary, no version field.
			p.observer.ClientPacket("ReadTaskResponse")
			p.stats.inc("ReadTaskResponse")
			response, err := chReader.Str()
			if err != nil {
				log.Infof("[conn %d] streaming: ReadTaskResponse response error: %v", id, err)
				return
			}
			rbuf := p.getBuffer() // P2-10: Uniformly use bufferPool
			rbuf.PutByte(byte(clientCodeReadTaskResponse))
			rbuf.PutString(response)
			if _, err := upstreamWriter.Write(rbuf.Buf); err != nil {
				p.putBuffer(rbuf)
				return
			}
			p.putBuffer(rbuf)

		case clientCodeMergeTreeReadTaskResponse:
			// R3-3: MergeTreeReadTaskResponse structured handling (replaces raw passthrough)
			// Format is identical to ReadTaskResponse/ClusterFunctionReadTaskResponse: [response: String]
			// Aligned with TCPHandler::receiveMergeTreeReadTaskResponse: readStringBinary(response)
			p.observer.ClientPacket("MergeTreeReadTaskResponse")
			p.stats.inc("MergeTreeReadTaskResponse")
			response, err := chReader.Str()
			if err != nil {
				log.Infof("[conn %d] streaming: MergeTreeReadTaskResponse decode error: %v", id, err)
				return
			}
			mbuf := p.getBuffer()
			mbuf.PutByte(byte(clientCodeMergeTreeReadTaskResponse))
			mbuf.PutString(response)
			if _, err := upstreamWriter.Write(mbuf.Buf); err != nil {
				p.putBuffer(mbuf)
				return
			}
			p.putBuffer(mbuf)

		case clientCodeClusterFunctionReadTaskResponse:
			// R1-5: ClusterFunctionReadTaskResponse (type 13)
			// Aligned with TCPHandler::receiveClusterFunctionReadTaskResponse
			// Format: [response: String]
			p.observer.ClientPacket("ClusterFunctionReadTaskResponse")
			p.stats.inc("ClusterFunctionReadTaskResponse")
			response, err := chReader.Str()
			if err != nil {
				log.Infof("[conn %d] streaming: ClusterFunctionReadTaskResponse decode error: %v", id, err)
				return
			}
			cbuf := p.getBuffer()
			cbuf.PutByte(byte(clientCodeClusterFunctionReadTaskResponse))
			cbuf.PutString(response)
			if _, err := upstreamWriter.Write(cbuf.Buf); err != nil {
				p.putBuffer(cbuf)
				return
			}
			p.putBuffer(cbuf)

		case clientCodeQueryPlan:
			// P0-3 Fix: QueryPlan also uses temporary raw passthrough
			p.observer.ClientPacket("QueryPlan")
			p.stats.inc("QueryPlan")
			log.Infof("[conn %d] streaming: QueryPlan packet detected, temporary raw passthrough", id)
			p.observer.Fallback("query_plan")
			if _, err := upstreamWriter.Write([]byte{codeByte}); err != nil {
				return
			}
			if !p.forwardUntilQueryDone(id, br, clientConn, upstreamWriter, queryDoneCh) {
				return
			}
			expectState = expectQuery
			queryCompression = proto.CompressionDisabled
			log.Infof("[conn %d] streaming: resumed streaming after QueryPlan", id)
			continue

		default:
			// Unknown packet type: cannot determine packet structure boundaries; use permanent fallback
			log.Warnf("[conn %d] streaming: unknown packet type %d, forwarding + permanent fallback", id, codeByte)
			p.observer.Fallback("unknown_packet")
			if _, err := upstreamWriter.Write([]byte{codeByte}); err != nil {
				return
			}
			p.fallbackRawCopy(id, br, clientConn, upstreamWriter)
			return
		}
	}
}

// forwardUntilQueryDone temporary raw passthrough mode: forwards client data to upstream chunk by chunk,
// until queryDoneCh receives a signal indicating the current query is done (upstream returned EndOfStream/Exception).
// Returns true when query is done and streaming can resume, false on connection error (should exit).
//
// R1-1 Fix: Use doneCh to control goroutine lifecycle, preventing goroutine leaks.
// When forwardUntilQueryDone returns, close doneCh to notify read goroutine to exit.
// Read goroutine checks doneCh before sending to readCh, avoiding sends to a channel with no consumer.
func (p *proxy) forwardUntilQueryDone(id int64, br *bufio.Reader, clientConn net.Conn, upstreamWriter io.Writer, queryDoneCh chan struct{}) bool {
	type readResult struct {
		data []byte
		err  error
	}
	readCh := make(chan readResult, 1)
	// R1-1: doneCh is used to notify the read goroutine to exit
	doneCh := make(chan struct{})
	defer close(doneCh)

	// Start read goroutine
	go func() {
		buf := make([]byte, 64*1024)
		for {
			// R1-1: Check doneCh first; exit immediately if parent function has returned
			select {
			case <-doneCh:
				return
			default:
			}

			if p.cfg.IdleTimeout.Duration > 0 {
				_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
			}
			n, err := br.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				// R1-1: Check doneCh again before send, to avoid blocking on a channel with no consumer
				select {
				case readCh <- readResult{data: data}:
				case <-doneCh:
					return
				}
			}
			if err != nil {
				select {
				case readCh <- readResult{err: err}:
				case <-doneCh:
				}
				return
			}
		}
	}()

	for {
		select {
		case <-queryDoneCh:
			log.Infof("[conn %d] forwardUntilQueryDone: query done signal received, resuming streaming", id)
			// R3-1: Drain read-but-unsent data
			// Wait briefly to give the read goroutine time to send the last batch of data
			// R5-2: Don't use defer Stop(); explicitly Stop before each return
			// Avoid defer accumulation in long loops (though it only triggers once here)
			drainTimer := time.NewTimer(50 * time.Millisecond)
		drainLoop:
			for {
				select {
				case res := <-readCh:
					if res.err != nil {
						drainTimer.Stop()
						return false
					}
					if _, werr := upstreamWriter.Write(res.data); werr != nil {
						drainTimer.Stop()
						return false
					}
					// Reset timeout after receiving data; there may be more data
					drainTimer.Reset(50 * time.Millisecond)
				case <-drainTimer.C:
					// R3-1: Timeout, no more in-flight data
					break drainLoop
				}
			}
			drainTimer.Stop()
			return true

		case res := <-readCh:
			if res.err != nil {
				return false
			}
			if _, werr := upstreamWriter.Write(res.data); werr != nil {
				return false
			}
		}
	}
}

// fallbackRawCopy falls back to raw chunk-by-chunk forwarding mode (permanent).
// upstreamWriter may be a ChunkedWriter (when chunked protocol is enabled), ensuring fallback data also goes through the chunked layer.
func (p *proxy) fallbackRawCopy(id int64, br *bufio.Reader, clientConn net.Conn, upstreamWriter io.Writer) {
	log.Infof("[conn %d] falling back to raw copy mode", id)
	buf := make([]byte, 64*1024)
	for {
		if p.cfg.IdleTimeout.Duration > 0 {
			_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}
		n, err := br.Read(buf)
		if n > 0 {
			if _, werr := upstreamWriter.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
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
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

// authTokenKeys is the list of auth token setting keys to strip in streaming mode.
var authTokenKeys = map[string]bool{
	"x_auth_token":     true,
	"SQL_x_auth_token": true,
}

// stripAuthTokenSettings removes auth token related settings from new-format Settings.
// Signature verification is done on the proxy side; token should not be sent to ClickHouse Server.
func stripAuthTokenSettings(settings []proto.Setting) []proto.Setting {
	n := 0
	for _, s := range settings {
		if !authTokenKeys[s.Key] {
			settings[n] = s
			n++
		}
	}
	return settings[:n]
}

// stripAuthTokenOldSettings removes auth token related settings from old-format Settings.
func stripAuthTokenOldSettings(settings []OldSetting) []OldSetting {
	n := 0
	for _, s := range settings {
		if !authTokenKeys[s.Key] {
			settings[n] = s
			n++
		}
	}
	return settings[:n]
}

// replaceToken replaces all occurrences of a length-prefixed key with another key,
// adjusting the length prefix and shifting the byte slice accordingly.
// It searches for [UVarInt(len(oldKey))][oldKey] and replaces it with [UVarInt(len(newKey))][newKey].
func replaceToken(data []byte, oldKey, newKey string) []byte {
	// Construct search sequence
	oldKeyBytes := []byte(oldKey)
	oldLenBuf := make([]byte, binary.MaxVarintLen64)
	nOld := binary.PutUvarint(oldLenBuf, uint64(len(oldKeyBytes)))
	searchSeq := make([]byte, nOld+len(oldKeyBytes))
	copy(searchSeq, oldLenBuf[:nOld])
	copy(searchSeq[nOld:], oldKeyBytes)

	// Construct replace sequence
	newKeyBytes := []byte(newKey)
	newLenBuf := make([]byte, binary.MaxVarintLen64)
	nNew := binary.PutUvarint(newLenBuf, uint64(len(newKeyBytes)))
	replaceSeq := make([]byte, nNew+len(newKeyBytes))
	copy(replaceSeq, newLenBuf[:nNew])
	copy(replaceSeq[nNew:], newKeyBytes)

	return bytes.ReplaceAll(data, searchSeq, replaceSeq)
}

// R1-3 + R2-5: eraseTokenValue replaces the auth token setting key with promql_table,
// and zeroes out the value string content following the key (preserving length prefix and position, not changing total packet length).
// Format: [UVarInt(len(key))][key][UVarInt(len(value))][value]
// After replacement: [UVarInt(len("promql_table"))]["promql_table"][UVarInt(0)]
//
// R2-5 Security enhancement: after replaceToken replacement, when searching for promql_table,
// verify it must have the correct UVarInt length prefix before it, preventing matches against literals in SQL text.
func eraseTokenValue(data []byte, tokenKey string) []byte {
	// First use replaceToken to replace the key name
	newKey := "promql_table"
	data = replaceToken(data, tokenKey, newKey)

	// Then find the replaced key and erase its subsequent value
	newKeyBytes := []byte(newKey)
	newLenBuf := make([]byte, binary.MaxVarintLen64)
	nNew := binary.PutUvarint(newLenBuf, uint64(len(newKeyBytes)))
	searchSeq := make([]byte, nNew+len(newKeyBytes))
	copy(searchSeq, newLenBuf[:nNew])
	copy(searchSeq[nNew:], newKeyBytes)

	// R4-3: Find promql_table and erase its subsequent value (refactored: eliminated for+break anti-pattern)
	idx := bytes.Index(data, searchSeq)
	if idx >= 0 {
		// R2-5: searchSeq already contains UVarInt length prefix + key content,
		// so it won't match bare "promql_table" strings in SQL text.
		valueStart := idx + len(searchSeq)
		if valueStart < len(data) {
			// Read the UVarInt length prefix of the value
			valLen, n := binary.Uvarint(data[valueStart:])
			const maxTokenValueLen = 4096
			if n > 0 && valueStart+n+int(valLen) <= len(data) && valLen <= maxTokenValueLen {
				// Zero out value content (sanitize), preserving overall structure
				for i := 0; i < int(valLen); i++ {
					data[valueStart+n+i] = '*'
				}
			}
		}
	}

	return data
}

func printStats(stats *packetStats) {
	snap := stats.snapshot()
	log.Infof("==== clickhouse-proxy stats ====")
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
		if loc := useRegexp.FindStringIndex(clean); len(loc) == 2 {
			clean = clean[loc[0]:]
		}
	}
	if len(clean) > maxLen {
		clean = clean[:maxLen]
	}
	return strings.TrimSpace(clean)
}
