package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// ClickHouse Chunked 协议帧格式（来自 ClickHouse C++ 源码 ReadBufferFromPocoSocketChunked.h）：
//
//   Chunk begins   | [4 bytes LE: chunk_size]   ← uint32 小端序，表示本帧数据长度
//                  |   packet_type + packet_data
//                  |   ... 可包含多个 packet ...
//   Chunk ends     | [4 bytes: 0x00000000]      ← 4 字节零结束标记
//
// 三种模式：
//   Basic:      单帧包含一个 packet
//   Datastream: 单帧包含多个连续 packet
//   Multipart:  packet 数据跨越多个帧（chunk part）

const (
	// chunkedHeaderSize 是 chunk 帧头的大小（4 字节 uint32 LE）
	chunkedHeaderSize = 4
	// chunkedEndMarker 是 chunk 结束标记值
	chunkedEndMarker = uint32(0)
	// maxChunkSize 是合理的最大 chunk 大小（防御性限制，256MB）
	maxChunkSize = 256 * 1024 * 1024
	// defaultChunkPayloadSize 是 ChunkedWriter 默认的 chunk 载荷大小
	defaultChunkPayloadSize = 64 * 1024
)

// ChunkedReader 从底层 io.Reader 中剥离 chunked 帧头和结束标记，
// 对上层暴露"裸"的协议数据流，使现有包解析逻辑无需任何修改。
//
// 实现 io.Reader 接口。当 enabled=false 时直接透传底层 Reader。
type ChunkedReader struct {
	r         io.Reader
	enabled   bool
	remaining int // 当前帧剩余未读字节数
	mu        sync.Mutex
}

// NewChunkedReader 创建 ChunkedReader。enabled 控制是否启用 chunked 解帧。
func NewChunkedReader(r io.Reader, enabled bool) *ChunkedReader {
	return &ChunkedReader{
		r:       r,
		enabled: enabled,
	}
}

// Enabled 返回当前是否启用 chunked 解帧。
func (cr *ChunkedReader) Enabled() bool {
	return cr.enabled
}

// Read 实现 io.Reader。
// 当 enabled=true 时，自动剥离 chunk 帧头和结束标记。
// 当 enabled=false 时，直接透传底层 Reader。
func (cr *ChunkedReader) Read(p []byte) (int, error) {
	if !cr.enabled {
		return cr.r.Read(p)
	}

	cr.mu.Lock()
	defer cr.mu.Unlock()

	for {
		if cr.remaining > 0 {
			// 当前帧还有数据可读
			toRead := len(p)
			if toRead > cr.remaining {
				toRead = cr.remaining
			}
			n, err := cr.r.Read(p[:toRead])
			cr.remaining -= n
			if n > 0 {
				return n, err
			}
			if err != nil {
				return 0, err
			}
			continue
		}

		// 需要读取下一个 chunk header
		size, err := cr.readChunkHeader()
		if err != nil {
			return 0, err // io.EOF 会被正确传播
		}

		if size == 0 {
			// chunk 结束标记（0x00000000），继续读取下一个 chunk
			continue
		}

		cr.remaining = int(size)
		// 回到循环顶部，从新帧中读取数据
	}
}

// readChunkHeader 读取 4 字节 chunk 帧头，返回 chunk 大小。
// 如果底层 Reader 已到达 EOF，返回 (0, io.EOF)。
func (cr *ChunkedReader) readChunkHeader() (uint32, error) {
	var header [chunkedHeaderSize]byte
	_, err := io.ReadFull(cr.r, header[:])
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("chunked: read frame header: %w", err)
	}
	size := binary.LittleEndian.Uint32(header[:])
	if size > uint32(maxChunkSize) {
		return 0, fmt.Errorf("chunked: frame size %d exceeds max %d", size, maxChunkSize)
	}
	return size, nil
}

// ChunkedWriter 将写入的数据用 chunked 帧格式包裹后发送到底层 Writer。
//
// 每次 Write 调用产生一个 chunk：[4 bytes LE: size][data][4 bytes: 0x00000000]
// 当 enabled=false 时直接透传到底层 Writer。
type ChunkedWriter struct {
	w       io.Writer
	enabled bool
	mu      sync.Mutex
}

// NewChunkedWriter 创建 ChunkedWriter。enabled 控制是否启用 chunked 封帧。
func NewChunkedWriter(w io.Writer, enabled bool) *ChunkedWriter {
	return &ChunkedWriter{
		w:       w,
		enabled: enabled,
	}
}

// Enabled 返回当前是否启用 chunked 封帧。
func (cw *ChunkedWriter) Enabled() bool {
	return cw.enabled
}

// Write 实现 io.Writer。
// 当 enabled=true 时，将数据包裹在 chunk 帧中：[size: 4 bytes LE][data][end: 4 bytes 0x00]
// 当 enabled=false 时，直接透传到底层 Writer。
func (cw *ChunkedWriter) Write(p []byte) (int, error) {
	if !cw.enabled {
		return cw.w.Write(p)
	}
	if len(p) == 0 {
		return 0, nil
	}

	cw.mu.Lock()
	defer cw.mu.Unlock()

	// 写入 chunk header: [4 bytes LE: size]
	var header [chunkedHeaderSize]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(p)))
	if _, err := cw.w.Write(header[:]); err != nil {
		return 0, fmt.Errorf("chunked: write frame header: %w", err)
	}

	// 写入数据
	n, err := cw.w.Write(p)
	if err != nil {
		return n, fmt.Errorf("chunked: write frame data: %w", err)
	}

	// 写入 chunk 结束标记: [4 bytes: 0x00000000]
	var endMarker [chunkedHeaderSize]byte
	if _, err := cw.w.Write(endMarker[:]); err != nil {
		return n, fmt.Errorf("chunked: write frame end marker: %w", err)
	}

	return n, nil
}

// chunkedNegotiate 根据双方的 chunked 能力协商实际使用模式。
// 返回值：是否启用 chunked 传输。
//
// 规则：
//   - 如果任一方声明 "notchunked" 或空字符串，则不启用
//   - 如果双方都声明 "chunked"，则启用
func chunkedNegotiate(senderCap, receiverCap string) bool {
	return senderCap == "chunked" && receiverCap == "chunked"
}
