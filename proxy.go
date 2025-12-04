package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

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
			log.Printf("accept error: %v", err)
			continue
		}

		id := atomic.AddInt64(&connID, 1)
		log.Printf("[conn %d] new connection from %s", id, clientConn.RemoteAddr())

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
		log.Printf("[conn %d] dial upstream %s error: %v", id, p.cfg.Upstream, err)
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
	log.Printf("[conn %d] closed", id)
}

func (p *proxy) copyUpstreamToClient(id int64, clientConn, upstreamConn net.Conn) {
	buf := make([]byte, 64*1024)
	for {
		if p.cfg.IdleTimeout.Duration > 0 {
			_ = upstreamConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}
		n, err := upstreamConn.Read(buf)
		if n > 0 {
			if p.cfg.IdleTimeout.Duration > 0 {
				_ = clientConn.SetWriteDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
			}
			if _, werr := clientConn.Write(buf[:n]); werr != nil {
				log.Printf("[conn %d] upstream->client write error: %v", id, werr)
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !isTimeout(err) {
				log.Printf("[conn %d] upstream->client read error: %v", id, err)
			}
			return
		}
	}
}

func (p *proxy) copyClientToUpstream(ctx context.Context, id int64, clientConn, upstreamConn net.Conn) {
	buf := make([]byte, 64*1024)
	for {
		if p.cfg.IdleTimeout.Duration > 0 {
			_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}
		n, readErr := clientConn.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			pkt := detectPacketType(chunk)
			p.stats.inc(pkt)
			p.logPacket(id, pkt, chunk)

			if pkt == "Query" {
				meta := QueryMeta{
					ConnID:       id,
					ClientAddr:   clientConn.RemoteAddr().String(),
					UpstreamAddr: p.cfg.Upstream,
					QueryPreview: summarizePrintable(chunk, p.cfg.MaxQueryLogBytes),
					Raw:          append([]byte(nil), chunk...),
				}
				if err := p.validator.ValidateQuery(ctx, meta); err != nil {
					log.Printf("[conn %d] query rejected: %v", id, err)
					return
				}
			}

			if p.cfg.IdleTimeout.Duration > 0 {
				_ = upstreamConn.SetWriteDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
			}
			if _, err := upstreamConn.Write(chunk); err != nil {
				log.Printf("[conn %d] client->upstream write error: %v", id, err)
				return
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && !isTimeout(readErr) {
				log.Printf("[conn %d] client->upstream read error: %v", id, readErr)
			}
			return
		}
	}
}

func (p *proxy) logPacket(id int64, pktType string, chunk []byte) {
	switch pktType {
	case "Query":
		if p.cfg.LogQueries {
			log.Printf("[conn %d] Query packet (%d bytes): %s", id, len(chunk), summarizePrintable(chunk, p.cfg.MaxQueryLogBytes))
		}
	case "Data":
		if p.cfg.LogData {
			log.Printf("[conn %d] Data packet (%d bytes): %s", id, len(chunk), summarizePrintable(chunk, p.cfg.MaxDataLogBytes))
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
	log.Printf("==== ck_remote_proxy stats ====")
	for _, key := range []string{"Hello", "Query", "Data", "Ping", "Cancel", "TablesStatusRequest", "KeepAlive", "Scalar", "Poll", "Data (portable)", "unknown"} {
		log.Printf("%-18s: %d", key, snap[key])
	}
	// Print any extra ids that appeared.
	for k, v := range snap {
		if _, known := packetNamesByName[k]; known {
			continue
		}
		if k == "unknown" {
			continue
		}
		log.Printf("%-18s: %d", k, v)
	}
	log.Printf("===============================")
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
