package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// chunkedFramePool 缓存 ChunkedWriter 的帧缓冲区，减少高频写入的 GC 压力
var chunkedFramePool = sync.Pool{
	New: func() interface{} {
		// 默认分配 64KB + 8 字节帧头/尾 的缓冲区
		buf := make([]byte, 0, defaultChunkPayloadSize+chunkedHeaderSize*2)
		return buf
	},
}

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
// 注意：ChunkedReader 不是并发安全的。调用方必须保证单 goroutine 访问。
type ChunkedReader struct {
	r         io.Reader
	enabled   bool
	remaining int // 当前帧剩余未读字节数
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
// 注意：ChunkedWriter 不是并发安全的。调用方必须保证单 goroutine 访问。
type ChunkedWriter struct {
	w       io.Writer
	enabled bool
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
// 优化：将 header + data + endMarker 合并为单次 Write 调用，减少系统调用开销。
// P2 #8: 使用 sync.Pool 缓存帧缓冲区，减少高频小包写入的内存分配。
func (cw *ChunkedWriter) Write(p []byte) (int, error) {
	if !cw.enabled {
		return cw.w.Write(p)
	}
	if len(p) == 0 {
		return 0, nil
	}

	// 合并为一次写入: [header:4][data:N][endMarker:4]
	frameSize := chunkedHeaderSize + len(p) + chunkedHeaderSize
	frame := chunkedFramePool.Get().([]byte)
	if cap(frame) < frameSize {
		frame = make([]byte, frameSize)
	} else {
		frame = frame[:frameSize]
	}
	binary.LittleEndian.PutUint32(frame[:chunkedHeaderSize], uint32(len(p)))
	copy(frame[chunkedHeaderSize:], p)
	// endMarker 位于 frame[chunkedHeaderSize+len(p):]
	binary.LittleEndian.PutUint32(frame[chunkedHeaderSize+len(p):], 0)

	_, err := cw.w.Write(frame)

	if err != nil {
		// P1-2: Write 失败时不归还 buffer 到 pool，避免脏数据残留
		return 0, fmt.Errorf("chunked: write frame: %w", err)
	}

	// 归还到 pool（超大帧不放回，避免 pool 中堆积大 buffer）
	const maxPoolFrameSize = 256 * 1024 // 256KB
	if cap(frame) <= maxPoolFrameSize {
		chunkedFramePool.Put(frame)
	}

	return len(p), nil
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
