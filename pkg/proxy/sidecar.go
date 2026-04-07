package proxy

// sidecar.go — Sidecar proxy mode implementation.
//
// In sidecar mode, the proxy sits next to the ClickHouse client (same Pod / same machine).
// It intercepts Query packets, signs them with a JWS token using the sidecar's own private key,
// and forwards all traffic to a remote server-side proxy.
//
// Data flow:
//   [ClickHouse Client] → [Sidecar Proxy] → [Server-side Proxy] → [ClickHouse Server]
//                        ← (responses flow back the same TCP path) ←
//
// The sidecar does NOT:
//   - Validate JWS tokens (that's the server-side proxy's job)
//   - Rewrite SQL (that's the server-side proxy's job)
//   - Access NetworkState / Redis
//   - Track query usage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	log "sentioxyz/sentio-core/common/log"
)

// handleSidecarConnection handles a client connection in sidecar mode.
// It transparently proxies ClickHouse native TCP protocol while injecting
// JWS tokens into every Query packet.
func (p *Proxy) handleSidecarConnection(ctx context.Context, id int64, clientConn net.Conn) {
	if p.sidecarSigner == nil {
		log.Errorf("[conn %d] sidecar: no sidecar signer configured, closing connection", id)
		return
	}

	bufSize := p.cfg.StreamingBufSize
	if bufSize <= 0 {
		bufSize = defaultStreamingBufSize
	}

	br := bufio.NewReaderSize(clientConn, bufSize)

	// ========== Phase 1: Hello Handshake ==========
	if p.cfg.IdleTimeout.Duration > 0 {
		_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
	}

	// Read Hello type byte
	typeByte, err := br.ReadByte()
	if err != nil {
		log.Warnf("[conn %d] sidecar: read hello type error: %v", id, err)
		return
	}
	if typeByte != byte(proto.ClientCodeHello) {
		log.Warnf("[conn %d] sidecar: expected Hello(0), got %d", id, typeByte)
		return
	}

	// Use TeeReader to capture Hello raw bytes for forwarding
	var helloBuf bytes.Buffer
	teeReader := io.TeeReader(br, &helloBuf)
	teeBr := bufio.NewReaderSize(teeReader, bufSize)
	teeChReader := proto.NewReader(teeBr)

	var hello proto.ClientHello
	if err := hello.Decode(teeChReader); err != nil {
		log.Warnf("[conn %d] sidecar: Hello decode error: %v", id, err)
		return
	}
	clientRevision := hello.ProtocolVersion
	log.Infof("[conn %d] sidecar: Hello decoded, client=%s user=%s revision=%d", id, hello.Name, hello.User, clientRevision)

	handshakeStart := time.Now()

	// Construct original Hello bytes (type byte + payload)
	helloToSend := make([]byte, 1+helloBuf.Len())
	helloToSend[0] = typeByte
	copy(helloToSend[1:], helloBuf.Bytes())

	// Dial upstream (server-side proxy)
	dialer := &net.Dialer{
		Timeout:   p.cfg.DialTimeout.Duration,
		KeepAlive: 30 * time.Second,
	}
	upstreamConn, err := dialer.DialContext(ctx, "tcp", p.cfg.SidecarUpstream)
	if err != nil {
		log.Errorf("[conn %d] sidecar: dial upstream %s error: %v", id, p.cfg.SidecarUpstream, err)
		p.observer.Error("dial", err)
		return
	}
	defer upstreamConn.Close()
	if tc, ok := upstreamConn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
		tc.SetNoDelay(true)
	}

	// Send Hello to upstream
	if _, err := upstreamConn.Write(helloToSend); err != nil {
		log.Errorf("[conn %d] sidecar: write hello error: %v", id, err)
		return
	}

	// ========== Phase 1.5: ServerHello ==========
	upBr := bufio.NewReaderSize(upstreamConn, bufSize)

	if p.cfg.IdleTimeout.Duration > 0 {
		_ = upstreamConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
	}

	// Use TeeReader to record ServerHello raw bytes for transparent forwarding
	var serverHelloRaw bytes.Buffer
	teeUpReader := io.TeeReader(upBr, &serverHelloRaw)
	teeUpBr := bufio.NewReaderSize(teeUpReader, bufSize)
	teeUpChReader := proto.NewReader(teeUpBr)

	// Read packet type
	pktType, err := teeUpChReader.UVarInt()
	if err != nil {
		log.Errorf("[conn %d] sidecar: read ServerHello packet_type error: %v", id, err)
		return
	}
	if pktType != 0 {
		log.Errorf("[conn %d] sidecar: expected ServerHello (type 0), got type %d", id, pktType)
		// Forward whatever we got to client (likely an Exception)
		if buffered := teeUpBr.Buffered(); buffered > 0 {
			drainBuf := make([]byte, buffered)
			teeUpBr.Read(drainBuf)
		}
		clientConn.Write(serverHelloRaw.Bytes())
		return
	}

	// Read ServerHello fields
	serverName, err := teeUpChReader.Str()
	if err != nil {
		log.Errorf("[conn %d] sidecar: read ServerHello name error: %v", id, err)
		return
	}
	major, err := teeUpChReader.UVarInt()
	if err != nil {
		log.Errorf("[conn %d] sidecar: read ServerHello major error: %v", id, err)
		return
	}
	minor, err := teeUpChReader.UVarInt()
	if err != nil {
		log.Errorf("[conn %d] sidecar: read ServerHello minor error: %v", id, err)
		return
	}
	serverRevUint, err := teeUpChReader.UVarInt()
	if err != nil {
		log.Errorf("[conn %d] sidecar: read ServerHello revision error: %v", id, err)
		return
	}
	serverRevision := int(serverRevUint)
	log.Infof("[conn %d] sidecar: ServerHello: name=%s version=%d.%d revision=%d", id, serverName, major, minor, serverRevision)

	// Read conditional ServerHello fields (based on clientRevision)
	if proto.FeatureVersionedParallelReplicas.In(clientRevision) {
		if _, err := teeUpChReader.UVarInt(); err != nil {
			log.Warnf("[conn %d] sidecar: read ServerHello parallel_replicas_version error: %v", id, err)
			return
		}
	}
	if proto.FeatureTimezone.In(clientRevision) {
		if _, err := teeUpChReader.Str(); err != nil {
			log.Warnf("[conn %d] sidecar: read ServerHello timezone error: %v", id, err)
			return
		}
	}
	if proto.FeatureDisplayName.In(clientRevision) {
		if _, err := teeUpChReader.Str(); err != nil {
			log.Warnf("[conn %d] sidecar: read ServerHello display_name error: %v", id, err)
			return
		}
	}
	if proto.FeatureVersionPatch.In(clientRevision) {
		if _, err := teeUpChReader.UVarInt(); err != nil {
			log.Warnf("[conn %d] sidecar: read ServerHello version_patch error: %v", id, err)
			return
		}
	}
	// Chunked protocol negotiation
	var srvSendChunked, srvRecvChunked string
	if proto.FeatureChunkedPackets.In(clientRevision) {
		srvSendChunked, err = teeUpChReader.Str()
		if err != nil {
			log.Warnf("[conn %d] sidecar: read ServerHello proto_send_chunked error: %v", id, err)
			return
		}
		srvRecvChunked, err = teeUpChReader.Str()
		if err != nil {
			log.Warnf("[conn %d] sidecar: read ServerHello proto_recv_chunked error: %v", id, err)
			return
		}
		log.Infof("[conn %d] sidecar: ServerHello chunked: send=%q recv=%q", id, srvSendChunked, srvRecvChunked)
	}

	// Drain remaining buffered ServerHello data
	for {
		buffered := teeUpBr.Buffered()
		if buffered <= 0 {
			break
		}
		drainBuf := make([]byte, buffered)
		n, err := teeUpBr.Read(drainBuf)
		if n > 0 {
			log.Infof("[conn %d] sidecar: ServerHello tail drained: %d bytes", id, n)
		}
		if err != nil {
			break
		}
	}

	// Forward complete ServerHello to client
	if _, err := clientConn.Write(serverHelloRaw.Bytes()); err != nil {
		log.Errorf("[conn %d] sidecar: write ServerHello to client error: %v", id, err)
		return
	}
	log.Infof("[conn %d] sidecar: ServerHello forwarded (%d bytes)", id, serverHelloRaw.Len())

	// Negotiated revision
	revision := clientRevision
	if serverRevision > 0 && serverRevision < revision {
		revision = serverRevision
	}
	log.Infof("[conn %d] sidecar: negotiated revision=%d (client=%d, server=%d)", id, revision, clientRevision, serverRevision)

	// ========== Phase 1.6: Addendum ==========
	addendumRevision := serverRevision
	var clientSendChunked, clientRecvChunked string

	// Re-create chReader from br (original client bufio.Reader, past the Hello)
	chReader := proto.NewReader(br)

	if proto.FeatureAddendum.In(addendumRevision) && proto.FeatureAddendum.In(clientRevision) {
		if p.cfg.IdleTimeout.Duration > 0 {
			_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}

		// quota_key
		quotaKey, err := chReader.Str()
		if err != nil {
			log.Infof("[conn %d] sidecar: read addendum quota_key error: %v", id, err)
			return
		}

		abuf := &proto.Buffer{}
		abuf.PutString(quotaKey)

		// chunked protocol negotiation
		if proto.FeatureChunkedPackets.In(addendumRevision) && proto.FeatureChunkedPackets.In(clientRevision) {
			clientSendChunked, err = chReader.Str()
			if err != nil {
				log.Warnf("[conn %d] sidecar: read addendum proto_send_chunked error: %v", id, err)
				return
			}
			clientRecvChunked, err = chReader.Str()
			if err != nil {
				log.Warnf("[conn %d] sidecar: read addendum proto_recv_chunked error: %v", id, err)
				return
			}
			log.Infof("[conn %d] sidecar: client chunked: send=%q recv=%q", id, clientSendChunked, clientRecvChunked)
			abuf.PutString(clientSendChunked)
			abuf.PutString(clientRecvChunked)
		}

		// parallel replicas version
		if proto.FeatureVersionedParallelReplicas.In(addendumRevision) && proto.FeatureVersionedParallelReplicas.In(clientRevision) {
			parallelVersion, err := chReader.UVarInt()
			if err != nil {
				log.Infof("[conn %d] sidecar: read addendum parallel_replicas_version error: %v", id, err)
				return
			}
			abuf.PutUVarInt(parallelVersion)
		}

		if _, err := upstreamConn.Write(abuf.Buf); err != nil {
			log.Infof("[conn %d] sidecar: write addendum error: %v", id, err)
			return
		}
		log.Infof("[conn %d] sidecar: addendum forwarded", id)
	}

	p.observer.HandshakeCompleted(time.Since(handshakeStart).Seconds())

	// ========== Phase 2: Bidirectional Packet Loop ==========
	// Setup chunked adapters if needed
	clientChunkedEnabled := clientSendChunked == "chunked"
	upstreamChunkedEnabled := srvRecvChunked == "chunked"
	srvChunkedEnabled := srvSendChunked == "chunked"
	clientRecvChunkedEnabled := clientRecvChunked == "chunked"

	if clientChunkedEnabled || upstreamChunkedEnabled {
		log.Infof("[conn %d] sidecar: enabling chunked adapters (client→proxy: %v, proxy→upstream: %v)",
			id, clientChunkedEnabled, upstreamChunkedEnabled)
	}
	if clientChunkedEnabled {
		chunkedClientReader := NewChunkedReader(br, true)
		br = bufio.NewReaderSize(chunkedClientReader, bufSize)
		chReader = proto.NewReader(br)
	}
	var upstreamWriter io.Writer = upstreamConn
	if upstreamChunkedEnabled {
		upstreamWriter = NewChunkedWriter(upstreamConn, true)
	}

	// Upstream → Client goroutine
	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			clientConn.Close()
			upstreamConn.Close()
		})
	}

	if p.cfg.MaxConnectionLifetime.Duration > 0 {
		lifetimeTimer := time.AfterFunc(p.cfg.MaxConnectionLifetime.Duration, func() {
			log.Infof("[conn %d] sidecar: max connection lifetime (%s) exceeded, closing", id, p.cfg.MaxConnectionLifetime.Duration)
			closeBoth()
		})
		defer lifetimeTimer.Stop()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Read from upstream, forward to client
		var reader io.Reader
		if srvChunkedEnabled {
			reader = NewChunkedReader(upBr, true)
		} else {
			reader = upBr
		}
		var writer io.Writer = clientConn
		if clientRecvChunkedEnabled {
			writer = NewChunkedWriter(clientConn, true)
		}

		buf := make([]byte, 64*1024)
		for {
			if p.cfg.IdleTimeout.Duration > 0 {
				_ = upstreamConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
			}
			n, err := reader.Read(buf)
			if n > 0 {
				p.observer.BytesTransferred("upstream_to_client", float64(n))
				if p.cfg.IdleTimeout.Duration > 0 {
					_ = clientConn.SetWriteDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
				}
				if _, werr := writer.Write(buf[:n]); werr != nil {
					log.Infof("[conn %d] sidecar: upstream→client write error: %v", id, werr)
					break
				}
			}
			if err != nil {
				if !errors.Is(err, io.EOF) && !isTimeout(err) && !errors.Is(err, net.ErrClosed) {
					log.Infof("[conn %d] sidecar: upstream→client read error: %v", id, err)
				}
				break
			}
		}
		closeBoth()
	}()

	// Client → Upstream packet loop (main goroutine)
	// Track compression state for Data block handling
	var queryCompression proto.Compression

	for {
		if p.cfg.IdleTimeout.Duration > 0 {
			_ = clientConn.SetReadDeadline(time.Now().Add(p.cfg.IdleTimeout.Duration))
		}

		codeByte, err := br.ReadByte()
		if err != nil {
			if !errors.Is(err, io.EOF) && !isTimeout(err) && !errors.Is(err, net.ErrClosed) {
				log.Infof("[conn %d] sidecar: read packet type error: %v", id, err)
			}
			break
		}
		code := proto.ClientCode(codeByte)

		switch code {
		case proto.ClientCodeQuery:
			p.observer.ClientPacket("Query")
			p.stats.inc("Query")

			// Decode Query
			eq, err := decodeQueryCustom(chReader, revision)
			if err != nil {
				log.Warnf("[conn %d] sidecar: Query decode error: %v, falling back to raw", id, err)
				// Write the code byte and fall back to raw copy
				upstreamWriter.Write([]byte{codeByte})
				p.fallbackRawCopy(id, br, clientConn, upstreamWriter)
				goto done
			}

			if p.cfg.LogQueries {
				log.Infof("[conn %d] sidecar: Query: %q", id, eq.Body)
			}

			// Sign and inject JWS token
			token, err := p.sidecarSigner.SignToken(eq.Body)
			if err != nil {
				log.Warnf("[conn %d] sidecar: JWS token signing failed: %v, forwarding without token", id, err)
				p.observer.SidecarTokenError()
			} else {
				eq.Settings = append(eq.Settings, proto.Setting{
					Key:   AuthTokenSettingKey,
					Value: token,
				})
				p.observer.SidecarTokenInjected()
				log.Infof("[conn %d] sidecar: JWS token injected (signer=%s, sql_len=%d)",
					id, p.sidecarSigner.Address(), len(eq.Body))
			}

			// Re-encode and forward
			qbuf := p.getBuffer()
			encodeQueryCustom(qbuf, eq, revision)
			if _, err := upstreamWriter.Write(qbuf.Buf); err != nil {
				p.putBuffer(qbuf)
				log.Infof("[conn %d] sidecar: write query error: %v", id, err)
				goto done
			}
			p.putBuffer(qbuf)
			p.observer.QueryForwarded()

			// Save compression state for subsequent Data blocks
			queryCompression = eq.Compression

		case proto.ClientCodeData:
			p.observer.ClientPacket("Data")
			p.stats.inc("Data")
			if err := p.handleDataBlock(ctx, id, proto.ClientCodeData, chReader, br, upstreamWriter, queryCompression, revision); err != nil {
				log.Infof("[conn %d] sidecar: Data block error: %v", id, err)
				goto done
			}

		case clientCodeScalar:
			p.observer.ClientPacket("Scalar")
			p.stats.inc("Scalar")
			if err := p.handleDataBlock(ctx, id, clientCodeScalar, chReader, br, upstreamWriter, queryCompression, revision); err != nil {
				log.Infof("[conn %d] sidecar: Scalar block error: %v", id, err)
				goto done
			}

		case proto.ClientCodePing:
			p.observer.ClientPacket("Ping")
			p.stats.inc("Ping")
			pbuf := p.getBuffer()
			proto.ClientCodePing.Encode(pbuf)
			if _, err := upstreamWriter.Write(pbuf.Buf); err != nil {
				p.putBuffer(pbuf)
				goto done
			}
			p.putBuffer(pbuf)

		case proto.ClientCodeCancel:
			p.observer.ClientPacket("Cancel")
			p.stats.inc("Cancel")
			cbuf := p.getBuffer()
			proto.ClientCodeCancel.Encode(cbuf)
			if _, err := upstreamWriter.Write(cbuf.Buf); err != nil {
				p.putBuffer(cbuf)
				goto done
			}
			p.putBuffer(cbuf)
			queryCompression = proto.CompressionDisabled

		case clientCodeKeepAlive:
			p.observer.ClientPacket("KeepAlive")
			p.stats.inc("KeepAlive")
			kbuf := p.getBuffer()
			kbuf.PutByte(byte(clientCodeKeepAlive))
			if _, err := upstreamWriter.Write(kbuf.Buf); err != nil {
				p.putBuffer(kbuf)
				goto done
			}
			p.putBuffer(kbuf)

		case proto.ClientTablesStatusRequest:
			p.observer.ClientPacket("TablesStatusRequest")
			p.stats.inc("TablesStatusRequest")
			numTables, err := chReader.UVarInt()
			if err != nil {
				log.Infof("[conn %d] sidecar: TablesStatusRequest decode error: %v", id, err)
				goto done
			}
			tbuf := p.getBuffer()
			proto.ClientTablesStatusRequest.Encode(tbuf)
			tbuf.PutUVarInt(numTables)
			for i := uint64(0); i < numTables; i++ {
				db, err := chReader.Str()
				if err != nil {
					p.putBuffer(tbuf)
					goto done
				}
				tbl, err := chReader.Str()
				if err != nil {
					p.putBuffer(tbuf)
					goto done
				}
				tbuf.PutString(db)
				tbuf.PutString(tbl)
			}
			if _, err := upstreamWriter.Write(tbuf.Buf); err != nil {
				p.putBuffer(tbuf)
				goto done
			}
			p.putBuffer(tbuf)

		default:
			// Unknown packet: forward type byte and fall back to raw copy
			log.Warnf("[conn %d] sidecar: unknown packet type %d, permanent fallback to raw copy", id, codeByte)
			p.observer.Fallback("unknown_packet")
			if _, err := upstreamWriter.Write([]byte{codeByte}); err != nil {
				goto done
			}
			p.fallbackRawCopy(id, br, clientConn, upstreamWriter)
			goto done
		}
	}

done:
	closeBoth()
	wg.Wait()
	log.Infof("[conn %d] sidecar: connection closed", id)
}

