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
	// R7-1: type 12 未被 ClickHouse TCPHandler 使用（保留），type 13 是 ClusterFunctionReadTaskResponse
	13: "ClusterFunctionReadTaskResponse",
}

// 预编译正则表达式，避免每次调用时重复编译
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

// ch-go v1.0.1 未定义的客户端包类型常量
const (
	clientCodeKeepAlive                 proto.ClientCode = 6
	clientCodeScalar                    proto.ClientCode = 7
	clientCodeIgnoredPartUUIDs          proto.ClientCode = 8
	clientCodeReadTaskResponse          proto.ClientCode = 9
	clientCodeMergeTreeReadTaskResponse proto.ClientCode = 10
	clientCodeQueryPlan                 proto.ClientCode = 11
	// R1-5: ClickHouse TCPHandler 中存在的 ClusterFunctionReadTaskResponse (type 13)
	clientCodeClusterFunctionReadTaskResponse proto.ClientCode = 13
)

// 协议与缓冲区相关常量
const (
	// fallbackRevision 在握手解码失败时使用的降级协议版本号
	fallbackRevision = 54423
	// uuidSize 是 ClickHouse UUID 的固定字节大小
	uuidSize = 16
	// defaultStreamingBufSize 是 streaming 模式下 bufio.Reader 的默认缓冲区大小 (128KB)
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
	// compressedBufPool 已移除：proto.Reader.ReadRaw 总是返回新分配的 slice，
	// 无法将数据读入 pool 获取的 buffer 中，导致 pool 形同虚设。
	bufferPool sync.Pool // 复用 proto.Buffer，减少包循环中的分配
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

// getBuffer 从 bufferPool 获取一个 proto.Buffer，并重置其内容。
func (p *proxy) getBuffer() *proto.Buffer {
	if v := p.bufferPool.Get(); v != nil {
		b := v.(*proto.Buffer)
		b.Reset()
		return b
	}
	return &proto.Buffer{}
}

// putBuffer 将 proto.Buffer 放回 bufferPool。超过 1MB 的不放回，避免大 buffer 堆积。
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
// R1-13: 使用原地移动替代新分配，减少 GC 压力
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
				// Hello 解析失败时不 disable parser，而是使用默认版本号继续
				// 这允许高版本客户端的 Query 仍能被解析
				log.Warnf("Hello decode failed (likely newer protocol): %v, using fallback version", err)
				p.version = fallbackRevision
				p.addendumDone = true
				p.resetBuf() // 清空缓冲区，跳过当前 Hello 包
				return out, nil
			}
			p.version = hello.ProtocolVersion
			consumed := n + cr.n
			p.consumeBuf(consumed)
		case 1: // Query
			if p.version == 0 {
				// 版本号为0说明 Hello 未被解析，尝试用通用版本降级
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
				// R5-5: 两种解码方式都失败，包含两个错误信息协助排查
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
			// R3-6: Cancel(3) 和 Ping(4) 均为无 payload 包（与 TCPHandler 一致）。
			// n 是 UVarInt 类型码本身的编码字节数（通常为 1 字节）。
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

// R1-16: 添加 graceful shutdown 排水机制
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

	// R1-16: 使用 WaitGroup 追踪在途连接，支持 graceful shutdown
	var connWg sync.WaitGroup

	var connID int64
	for {
		clientConn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				// 等待在途连接完成（带超时）
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
	// R7-2: 防止 panic 导致整个进程崩溃（与 R6-3 一致）
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
	// R6-3: 防止 panic 导致整个进程崩溃
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
		tc.SetNoDelay(true) // P2-12: 禁用 Nagle 算法，减少小包延迟
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
		tc.SetNoDelay(true) // P2-12: 禁用 Nagle 算法，减少小包延迟
	}

	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			clientConn.Close()
			upstreamConn.Close()
		})
	}

	// P2-6: 连接级别最大生存时间限制
	if p.cfg.MaxConnectionLifetime.Duration > 0 {
		lifetimeTimer := time.AfterFunc(p.cfg.MaxConnectionLifetime.Duration, func() {
			log.Infof("[conn %d] max connection lifetime (%s) exceeded, closing", id, p.cfg.MaxConnectionLifetime.Duration)
			closeBoth()
		})
		defer lifetimeTimer.Stop()
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// 默认使用 streaming 模式：精确解析协议、strip auth token settings、支持 SQL 重写。
	// 非 streaming 的 raw copy 模式仅保留作为未来 fallback。
	{
		// Streaming 模式：copyClientToUpstreamStreaming 需要先同步完成
		// Hello/ServerHello/Addendum 握手（从 upstream 读 ServerHello），
		// 之后才能启动 copyUpstreamToClient（避免两个 goroutine 同时读 upstream）。
		handshakeDone := make(chan struct{})
		// upstream 的 bufio.Reader，由 copyClientToUpstreamStreaming 设置
		var upstreamBr *bufio.Reader
		// queryDoneCh: upstream goroutine 在检测到 EndOfStream(5)/Exception(2) 时发送信号，
		// 包循环通过 select 非阻塞接收来重置压缩状态。
		// 缓冲区大小设为 8，确保多个快速连续的 EndOfStream 不会丢失信号。
		queryDoneCh := make(chan struct{}, 8)
		// chunked 协商结果，由 copyClientToUpstreamStreaming 在握手时设置
		var srvSendChunked, clientRecvChunked string

		go func() {
			defer wg.Done()
			// 等握手完成后再开始 upstream→client copy
			<-handshakeDone
			p.copyUpstreamToClientFromReader(id, clientConn, upstreamConn, upstreamBr, queryDoneCh, srvSendChunked, clientRecvChunked)
			closeBoth()
		}()

		go func() {
			defer wg.Done()
			p.copyClientToUpstreamStreaming(ctx, id, clientConn, upstreamConn, handshakeDone, &upstreamBr, queryDoneCh, &srvSendChunked, &clientRecvChunked)
			closeBoth()
		}()
	}

	wg.Wait()
	log.Infof("[conn %d] closed", id)
}

func (p *proxy) copyUpstreamToClient(id int64, clientConn, upstreamConn net.Conn) {
	p.copyUpstreamToClientFromReader(id, clientConn, upstreamConn, nil, nil, "", "")
}

// copyUpstreamToClientFromReader 从 upstream 读数据转发给 client。
// 如果 upstreamBr 不为 nil，从 bufio.Reader 读取（streaming 模式，防止丢失 ServerHello 后的缓存数据）。
func (p *proxy) copyUpstreamToClientFromReader(id int64, clientConn, upstreamConn net.Conn, upstreamBr *bufio.Reader, queryDoneCh chan struct{}, srvSendChunked, clientRecvChunked string) {
	// 根据 chunked 协商结果包裹 Reader/Writer
	// srvSendChunked: server 发送到 proxy 时是否用 chunked（需要 ChunkedReader 解帧）
	// clientRecvChunked: proxy 发送到 client 时 client 期望 chunked（需要 ChunkedWriter 封帧）
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
			// R2-6: 在 chunked 模式下，ChunkedReader 可能返回跨包边界的数据，
			// chunk[0] 不一定是包类型字节。检测结果是 best-effort 的。
			// EndOfStream(5) 和 Exception(2) 通常是短包且独占一个 chunk，
			// 所以大多数情况下检测有效，但偶尔可能误判。
			// TODO(R2-6): 对于高可靠性要求，应在 chunked 模式下使用结构化包边界解析。
			pkt := detectServerPacketType(chunk)
			if pkt != "unknown" {
				p.observer.ServerPacket(pkt)
			}

			// 检测 upstream EndOfStream(5)/Exception(2)，通知包循环重置压缩状态
			// 对齐 ClickHouse 客户端 Connection::receivePacket 的行为
			// 使用 buffered channel，非阻塞发送确保不丢失信号
			if queryDoneCh != nil && (pkt == "EndOfStream" || pkt == "Exception") {
				select {
				case queryDoneCh <- struct{}{}:
				default:
					// channel 满时跳过（不应该发生，因为 buffer=8）
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
			// 安全修复: 除了替换 key 名称外，还需要将 token 的值进行脱敏
			// 使用 eraseTokenValue 将 key 替换为 promql_table 并擦除值内容
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

// copyClientToUpstreamStreaming 使用 ch-go 官方协议库做流式解析和 SQL 重写。
// 精确解码 Hello/Query 包，完全消除裸字节扫描的分包和误匹配风险。
// handleDataBlock 统一处理压缩和非压缩的 Data/Scalar block。
// 压缩模式: raw passthrough — read compressed frame header to determine size, forward raw bytes
// 非压缩模式: 直接解码 BlockInfo + RawBlock → 编码
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
	// 读取 block_name（不压缩，始终在明文流中）
	blockName, err := chReader.Str()
	if err != nil {
		return fmt.Errorf("block name: %w", err)
	}

	// P3-4: 使用 bufferPool 减少头部编码的内存分配（Data 块是最频繁的路径）
	hdrBuf := p.getBuffer()
	hdrBuf.PutByte(byte(code))
	hdrBuf.PutString(blockName)

	// 检查 context 是否已取消
	if err := ctx.Err(); err != nil {
		p.putBuffer(hdrBuf)
		return fmt.Errorf("context cancelled: %w", err)
	}

	// 先写头部 (packet_code + block_name)
	if _, err := upstreamWriter.Write(hdrBuf.Buf); err != nil {
		p.putBuffer(hdrBuf)
		return fmt.Errorf("write block header: %w", err)
	}
	p.putBuffer(hdrBuf)

	if queryCompression == proto.CompressionEnabled {
		p.observer.StreamingDataBlock("compressed")
		// ========== 压缩模式: 流式逐帧 raw passthrough (多帧支持) ==========
		// ClickHouse compressed frame format:
		//   [16 bytes: CityHash128 checksum]
		//   [1 byte: compression method (0x82=LZ4, 0x90=ZSTD, 0x02=none)]
		//   [4 bytes LE: compressed_size (includes 9-byte sub-header + compressed data)]
		//   [4 bytes LE: decompressed_size]
		//   [N bytes: compressed data, where N = compressed_size - 9]
		// Total frame size = 16 (checksum) + compressed_size
		//
		// 重要：一个逻辑 Data Block 可能由多个压缩帧组成。
		// 当 Block 大小超过 DBMS_MAX_COMPRESSED_BLOCK_SIZE（默认 1MB）时，
		// ClickHouse 客户端的 CompressedWriteBuffer 会将数据分割为多个连续的压缩帧。
		// 我们从 stream 中循环读取所有连续的压缩帧，通过检测下一个帧的 method 字节判断边界。

		const frameHeaderSize = 16 + 1 + 4 + 4 // = 25 bytes
		const maxCompressedFrameSize = 32 * 1024 * 1024
		const maxDecompressedSize = 256 * 1024 * 1024
		totalFrameBytes := 0

		for {
			// 使用 chReader.ReadRaw 保证从同一缓冲层读取
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

			// R6-1: 使用 bufferPool 减少高频 INSERT 场景的压缩帧内存分配
			fBuf := p.getBuffer()
			fBuf.Buf = append(fBuf.Buf[:0], header...)
			fBuf.Buf = append(fBuf.Buf, compressedData...)
			if _, err := upstreamWriter.Write(fBuf.Buf); err != nil {
				p.putBuffer(fBuf)
				return fmt.Errorf("write compressed frame: %w", err)
			}
			p.putBuffer(fBuf)
			totalFrameBytes += frameHeaderSize + remainingDataSize

			// R1-4: 检测是否有后续压缩帧
			// 修复: 使用 chReader 的底层 bufio.Reader 进行 peek，确保与 ReadRaw 在同一缓冲层。
			// 在 chunked 模式下，br 可能是 ChunkedReader 之上的新 bufio.Reader，
			// 而 chReader 也在同一 bufio.Reader 之上，两者一致。
			// 注意: chReader.ReadRaw 最终从 br.Read 读取, 所以 br.Peek 与之一致。
			nextBytes, peekErr := br.Peek(frameHeaderSize) // 25 bytes
			if peekErr != nil || len(nextBytes) < frameHeaderSize {
				// 无法 peek 完整帧头，当前帧是最后一帧
				break
			}
			methodByte := nextBytes[16]
			if methodByte != 0x82 && methodByte != 0x90 && methodByte != 0x02 {
				// 不是压缩方法字节，说明下一个不是压缩帧
				break
			}
			// 额外验证: compressed_size >= 9 且 decompressed_size > 0 且两者在合理范围内
			peekCompSize := binary.LittleEndian.Uint32(nextBytes[17:21])
			peekDecompSize := binary.LittleEndian.Uint32(nextBytes[21:25])
			if peekCompSize < 9 || peekCompSize > maxCompressedFrameSize || peekDecompSize == 0 || peekDecompSize > maxDecompressedSize {
				// 不符合压缩帧的合理参数范围，视为非压缩帧
				break
			}
			// 继续读取下一个压缩帧
		}

		if p.cfg.LogQueries {
			log.Infof("[conn %d] streaming: forwarded compressed %s block (streaming passthrough, %d frame bytes)",
				id, code, totalFrameBytes)
		}
	} else {
		p.observer.StreamingDataBlock("uncompressed")
		// ========== 非压缩模式 ==========
		// 需要解码 BlockInfo 和 RawBlock 来确定块边界。
		blockInfo, err := decodeBlockInfoCompat(chReader)
		if err != nil {
			return fmt.Errorf("BlockInfo decode: %w", err)
		}

		// P3-5: 使用 bufferPool 减少编码缓冲区的内存分配
		encBuf := p.getBuffer()
		encodeBlockInfoCompat(encBuf, blockInfo)

		// P2-1: 空 Block 快速路径 — 跳过完整的 DecodeRawBlock/EncodeRawBlock
		// ClickHouse 在 Query 结束时发送空 Block (columns=0, rows=0) 作为结束标志。
		// 空 Block 占所有 Data 包的 ~50%，跳过完整的列解析+编码可显著减少开销。
		// 检测方法：Peek 前 2 字节，如果 num_columns 和 num_rows 的 UVarInt 都是 0，
		// 则不需要解码列数据。
		peekBytes, peekErr := br.Peek(2)
		if peekErr == nil && len(peekBytes) >= 2 && peekBytes[0] == 0 && peekBytes[1] == 0 {
			// 空 Block 快速路径：直接消费 2 字节 (num_columns=0, num_rows=0)
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
			// 非空 Block: 完整 decode-encode
			var block proto.Block
			var results proto.Results
			if err := block.DecodeRawBlock(chReader, revision, results.Auto()); err != nil {
				p.putBuffer(encBuf)
				return fmt.Errorf("block raw decode: %w", err)
			}

			if block.End() {
				// columns=0, rows=0 的情况（理论上被快速路径拦截了，这里是防御性处理）
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
	// 确保 handshakeDone 在函数退出时被关闭，避免 copyUpstreamToClient goroutine 永远阻塞
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

	// 创建 upstream 的 bufio.Reader（后续 copyUpstreamToClient 也从这里读）
	upBr := bufio.NewReaderSize(upstreamConn, bufSize)
	*upstreamBrOut = upBr

	// ========== Phase 1: Hello 握手 ==========
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

	// P1-3: 使用 TeeReader 记录 Hello 的原始字节，实现 raw passthrough
	// 避免 ClientHello.Encode() 可能遗漏新版本协议字段的风险
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

	// 记录握手开始时间
	handshakeStart := time.Now()

	// 将 Hello 类型字节 + 原始解码的字节一起发送给 upstream
	// helloBuf 包含了 TeeReader 记录的 Hello 所有原始字节（不含 typeByte）
	helloPayload := make([]byte, 1+helloBuf.Len())
	helloPayload[0] = typeByte
	copy(helloPayload[1:], helloBuf.Bytes())
	if _, err := upstreamConn.Write(helloPayload); err != nil {
		log.Infof("[conn %d] streaming: write hello error: %v", id, err)
		return
	}

	// ========== Phase 1.5: 同步读取并转发 ServerHello ==========
	// ClickHouse 客户端在 sendHello() 后调用 receiveHello()，
	// 收到 ServerHello 后才调用 sendAddendum()。
	// 因此 proxy 必须先拿到 ServerHello 转发给 client，
	// client 才会发 Addendum。
	//
	// ServerHello 格式（根据 client_tcp_protocol_version 条件包含不同字段）：
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

	// P0-1 Fix: 使用 TeeReader 记录 ServerHello 的全部原始字节
	// 好处：无论 ClickHouse 未来新增什么尾部字段（password_rules, nonce, settings,
	// query_plan_serialization_version 等），都会被自动记录并原样透传给客户端。
	// 消除了旧方案中 Buffered() 盲取在分包场景下丢失数据的风险。
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
	// P1 #4: 检查是否是 ServerHello (type 0)
	// 如果 upstream 返回 Exception (type 2)，直接转发给客户端
	if pktType != 0 {
		log.Errorf("[conn %d] streaming: expected ServerHello (type 0), got type %d", id, pktType)
		// serverHelloRaw 已经包含了 pktType 的字节
		// 再 drain teeUpBr 中剩余的缓冲数据（错误消息等）
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

	// 以下字段基于 clientRevision（服务端根据 client_tcp_protocol_version 条件发送）
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
	// chunked protocol negotiation (从 ServerHello 中读取服务端的 chunked caps)
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
		// 保存协商结果给 copyUpstreamToClientFromReader
		if srvSendChunkedOut != nil {
			*srvSendChunkedOut = srvSendChunked
		}
	}

	// P0-3 Fix: 将 teeUpBr 中剩余的缓冲数据完整 drain 到 serverHelloRaw。
	// 使用循环 drain 确保即使 bufio 内部分批返回数据也能全部读完。
	// 这些数据是 ServerHello 的尾部字段（password_rules, nonce, settings 等），
	// 已被底层 socket 或 bufio 预读但尚未被 teeReader 处理。
	// 必须在 close(handshakeDone) 之前完成，否则 copyUpstreamToClientFromReader
	// 启动后会用 ChunkedReader 解析这些非 chunked 的 ServerHello 尾部数据。
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

	// 将 TeeReader 记录的完整 ServerHello 原始字节发给客户端
	if _, err := clientConn.Write(serverHelloRaw.Bytes()); err != nil {
		log.Errorf("[conn %d] streaming: write ServerHello to client error: %v", id, err)
		return
	}
	log.Infof("[conn %d] streaming: ServerHello forwarded (%d bytes)", id, serverHelloRaw.Len())

	// 使用 min(clientRevision, serverRevision) 作为协商后的有效 revision
	revision := clientRevision
	if serverRevision > 0 && serverRevision < revision {
		revision = serverRevision
	}
	log.Infof("[conn %d] streaming: negotiated revision=%d (client=%d, server=%d)", id, revision, clientRevision, serverRevision)

	p.observer.HandshakeCompleted(time.Since(handshakeStart).Seconds())

	// 先释放握手锁让 copyUpstreamToClient 启动
	close(handshakeDone)
	handshakeClosed = true

	// ========== Phase 1.6: 处理 Addendum ==========
	// 注意：此时 copyUpstreamToClient 已启动，正在将 ServerHello 的剩余字段发给 client。
	// 客户端收到完整的 ServerHello 后会发 Addendum。
	// Addendum 字段基于 server_revision（客户端使用从 ServerHello 中读到的 revision）：
	//   1. quota_key: String (server_revision >= FeatureQuotaKey=54458)
	//   2. proto_send_chunked: String (server_revision >= FeatureChunkedPackets=54470)
	//   3. proto_recv_chunked: String (server_revision >= FeatureChunkedPackets=54470)
	//   4. parallel_replicas_version: UVarInt (server_revision >= FeatureVersionedParallelReplicas=54471)
	// 使用 serverRevision 作为判断条件（因为客户端的 sendAddendum 基于 server_revision）
	addendumRevision := serverRevision
	var clientSendChunked, clientRecvChunked string
	// P1 #5: 双重门控 — 客户端必须也支持 Addendum 才会发送
	// 客户端的 sendAddendum 基于 server_revision，但客户端代码版本太旧
	// 可能根本不知道 Addendum 协议，此时不应等待
	if proto.FeatureAddendum.In(addendumRevision) && proto.FeatureAddendum.In(clientRevision) {
		// 设置超时以等待客户端发送 Addendum
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

			// 透传客户端的 chunked 能力给 upstream
			abuf.PutString(clientSendChunked)
			abuf.PutString(clientRecvChunked)
			// 保存协商结果
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

	// ========== Phase 2: 包循环 ==========
	// Chunked 适配层（在握手完成后、包循环开始前插入）
	// 注意：ClickHouse 的 chunked 协议只在握手完成后启用，Hello/ServerHello/Addendum 始终不 chunked
	clientChunkedEnabled := clientSendChunked == "chunked"
	upstreamChunkedEnabled := srvRecvChunked == "chunked"
	if clientChunkedEnabled || upstreamChunkedEnabled {
		log.Infof("[conn %d] streaming: enabling chunked adapters (client→proxy: %v, proxy→upstream: %v)",
			id, clientChunkedEnabled, upstreamChunkedEnabled)
	}
	if clientChunkedEnabled {
		// 客户端发送 chunked 数据 → 用 ChunkedReader 包裹 br
		// 这样 br.ReadByte() 和 chReader 都能透明读到"裸"协议数据
		chunkedClientReader := NewChunkedReader(br, true)
		br = bufio.NewReaderSize(chunkedClientReader, bufSize)
		chReader = proto.NewReader(br)
	}
	var upstreamWriter io.Writer = upstreamConn
	if upstreamChunkedEnabled {
		// upstream 期望 chunked 数据 → 用 ChunkedWriter 包裹
		upstreamWriter = NewChunkedWriter(upstreamConn, true)
	}
	_ = upstreamWriter // 以下包循环 / handleDataBlock 通过它写入 upstream

	// 跟踪当前 Query 的压缩状态，用于决定 Data 块的处理方式
	var queryCompression proto.Compression

	// 两阶段包循环状态机，对齐 ClickHouse TCPHandler 的
	// receivePacketsExpectQuery / receivePacketsExpectData 模式。
	// expectQuery: 等待新 Query（只接受 Query/Ping/Cancel/TablesStatusRequest/IgnoredPartUUIDs）
	// expectData: Query 执行中（只接受 Data/Scalar/Ping/Cancel/ReadTaskResponse 等）
	const (
		expectQuery = 0
		expectData  = 1
	)
	expectState := expectQuery

	for {
		// 通过 channel 检测 upstream EndOfStream/Exception，重置压缩状态
		// 非阻塞 select：有信号就处理，没有就继续
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
			// 使用自定义 Query 解码器（支持所有协议版本，与 ClickHouse TCPHandler 精确对齐）
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

			// R4-2: Streaming 模式必须调用 Validator 进行签名验证
			// 非 streaming 模式在 copyClientToUpstream 中调用 p.validator.ValidateQuery，
			// streaming 模式之前缺失了这一步，导致 JWS 签名验证被绕过。
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

			// P2-1: 签名验证后截掉 auth token settings，不发送给 ClickHouse Server
			// 在 streaming 模式下，decode 后移除 token，encode 时自然不会包含
			eq.Settings = stripAuthTokenSettings(eq.Settings)
			eq.OldSettings = stripAuthTokenOldSettings(eq.OldSettings)

			// SQL 重写
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

			// 使用自定义编码器重编码 Query（与 decodeQueryCustom 严格镜像）
			qbuf := p.getBuffer()
			encodeQueryCustom(qbuf, eq, revision)
			if _, err := upstreamWriter.Write(qbuf.Buf); err != nil {
				p.putBuffer(qbuf)
				log.Infof("[conn %d] streaming: write query error: %v", id, err)
				return
			}
			p.putBuffer(qbuf)
			p.observer.QueryForwarded()

			// 保存压缩状态，影响后续 Data 块的处理方式
			queryCompression = eq.Compression
			expectState = expectData // 对齐 TCPHandler：Query 后进入 expectData 阶段

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
			// R2-1: TCPHandler::receivePacketsExpectQuery 中 Scalar 会抛 UNEXPECTED_PACKET 异常
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
			// P1-4: Cancel 后重置查询状态
			// 对齐 ClickHouse TCPHandler::processCancel 的行为：
			// Cancel 表示客户端要求中止当前查询，不应继续用旧的压缩状态
			expectState = expectQuery
			queryCompression = proto.CompressionDisabled

		case clientCodeKeepAlive:
			// KeepAlive 无 payload，只需转发类型码
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
			// R2-1: TCPHandler::receivePacketsExpectData 中 TablesStatusRequest 会抛 UNEXPECTED_PACKET 异常
			if expectState == expectData {
				log.Errorf("[conn %d] streaming: unexpected TablesStatusRequest in expectData state, closing (TCPHandler rejects)", id)
				return
			}
			p.observer.ClientPacket("TablesStatusRequest")
			p.stats.inc("TablesStatusRequest")
			// 结构: [num_tables: UVarInt] + [database: String, table: String] × N
			numTables, err := chReader.UVarInt()
			if err != nil {
				log.Infof("[conn %d] streaming: TablesStatusRequest decode error: %v", id, err)
				return
			}
			tbuf := p.getBuffer() // P2-10: 统一使用 bufferPool
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
			// R2-1: TCPHandler::receivePacketsExpectData 中 IgnoredPartUUIDs 会抛 UNEXPECTED_PACKET 异常
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
			ibuf := p.getBuffer() // P2-10: 统一使用 bufferPool
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
			// 根据 ClickHouse TCPHandler::receiveReadTaskResponseAssumeLocked，
			// 只有一个 readStringBinary，没有 version 字段。
			p.observer.ClientPacket("ReadTaskResponse")
			p.stats.inc("ReadTaskResponse")
			response, err := chReader.Str()
			if err != nil {
				log.Infof("[conn %d] streaming: ReadTaskResponse response error: %v", id, err)
				return
			}
			rbuf := p.getBuffer() // P2-10: 统一使用 bufferPool
			rbuf.PutByte(byte(clientCodeReadTaskResponse))
			rbuf.PutString(response)
			if _, err := upstreamWriter.Write(rbuf.Buf); err != nil {
				p.putBuffer(rbuf)
				return
			}
			p.putBuffer(rbuf)

		case clientCodeMergeTreeReadTaskResponse:
			// R3-3: MergeTreeReadTaskResponse 结构化处理（替代 raw passthrough）
			// 格式与 ReadTaskResponse/ClusterFunctionReadTaskResponse 完全相同: [response: String]
			// 对齐 TCPHandler::receiveMergeTreeReadTaskResponse: readStringBinary(response)
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
			// 与 TCPHandler::receiveClusterFunctionReadTaskResponse 对齐
			// 格式: [response: String]
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
			// P0-3 Fix: QueryPlan 也使用临时 raw passthrough
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
			// 未知包类型：无法确定包结构边界，仍使用永久 fallback
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

// forwardUntilQueryDone 临时 raw passthrough 模式：逐块转发客户端数据到 upstream，
// 直到 queryDoneCh 收到信号表明当前查询已完成（upstream 返回 EndOfStream/Exception）。
// 返回 true 表示查询完成可恢复 streaming，false 表示连接错误应退出。
//
// R1-1 Fix: 使用 doneCh 控制 goroutine 生命周期，防止 goroutine 泄漏。
// 当 forwardUntilQueryDone 返回时，关闭 doneCh 通知读取 goroutine 退出。
// 读取 goroutine 在发送到 readCh 前检查 doneCh，避免向已无消费者的 channel 发送数据。
func (p *proxy) forwardUntilQueryDone(id int64, br *bufio.Reader, clientConn net.Conn, upstreamWriter io.Writer, queryDoneCh chan struct{}) bool {
	type readResult struct {
		data []byte
		err  error
	}
	readCh := make(chan readResult, 1)
	// R1-1: doneCh 用于通知读取 goroutine 退出
	doneCh := make(chan struct{})
	defer close(doneCh)

	// 启动读取 goroutine
	go func() {
		buf := make([]byte, 64*1024)
		for {
			// R1-1: 先检查 doneCh，如果父函数已返回则立即退出
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
				// R1-1: 发送前再次检查 doneCh，防止阻塞在无消费者的 channel 上
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
			// R3-1: drain 已读但未发送的数据
			// 先等待一个短超时，给读取 goroutine 时间将最后一批数据发出
			// R5-2: 不使用 defer Stop()，在每个 return 前显式 Stop
			// 避免在长循环中 defer 累积（虽然此处实际只触发一次）
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
					// 收到数据后重置超时，可能还有更多数据
					drainTimer.Reset(50 * time.Millisecond)
				case <-drainTimer.C:
					// R3-1: 超时，不再有 in-flight 数据
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

// fallbackRawCopy 回退到原始逐 chunk 转发模式（永久性）。
// upstreamWriter 可能是 ChunkedWriter（当 chunked 协议启用时），确保 fallback 数据也通过 chunked 层。
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

// authTokenKeys 是需要在 streaming 模式下截掉的 auth token setting 键列表。
var authTokenKeys = map[string]bool{
	"x_auth_token":     true,
	"SQL_x_auth_token": true,
}

// stripAuthTokenSettings 从新格式 Settings 中移除 auth token 相关的 setting。
// 签名验证已在 proxy 端完成，token 不应发送给 ClickHouse Server。
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

// stripAuthTokenOldSettings 从旧格式 Settings 中移除 auth token 相关的 setting。
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

// R1-3 + R2-5: eraseTokenValue 将 auth token setting 的 key 替换为 promql_table，
// 并将紧跟 key 之后的 value 字符串内容清零（保留长度前缀和原始位置，不改变包总长度）。
// 格式: [UVarInt(len(key))][key][UVarInt(len(value))][value]
// 替换后: [UVarInt(len("promql_table"))]["promql_table"][UVarInt(0)]
//
// R2-5 安全增强：使用 replaceToken 替换后在搜索 promql_table 时，
// 验证其前面必须有正确的 UVarInt 长度前缀，防止匹配到 SQL 文本中的字面量。
func eraseTokenValue(data []byte, tokenKey string) []byte {
	// 首先用 replaceToken 替换 key 名称
	newKey := "promql_table"
	data = replaceToken(data, tokenKey, newKey)

	// 然后查找替换后的 key 并擦除其后的 value
	newKeyBytes := []byte(newKey)
	newLenBuf := make([]byte, binary.MaxVarintLen64)
	nNew := binary.PutUvarint(newLenBuf, uint64(len(newKeyBytes)))
	searchSeq := make([]byte, nNew+len(newKeyBytes))
	copy(searchSeq, newLenBuf[:nNew])
	copy(searchSeq[nNew:], newKeyBytes)

	// R4-3: 查找 promql_table 并擦除其后的 value（重构：消除 for+break 反模式）
	idx := bytes.Index(data, searchSeq)
	if idx >= 0 {
		// R2-5: searchSeq 已经包含了 UVarInt 长度前缀 + key 内容，
		// 不会匹配 SQL 文本中裸的 "promql_table" 字符串。
		valueStart := idx + len(searchSeq)
		if valueStart < len(data) {
			// 读取 value 的 UVarInt 长度前缀
			valLen, n := binary.Uvarint(data[valueStart:])
			const maxTokenValueLen = 4096
			if n > 0 && valueStart+n+int(valLen) <= len(data) && valLen <= maxTokenValueLen {
				// 将 value 内容清零（脱敏），保留整体结构不变
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
