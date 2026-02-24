package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// chunkedFramePool caches ChunkedWriter frame buffers to reduce GC pressure from high-frequency writes
var chunkedFramePool = sync.Pool{
	New: func() interface{} {
		// Allocate 64KB + 8 byte frame header/trailer buffer by default
		buf := make([]byte, 0, defaultChunkPayloadSize+chunkedHeaderSize*2)
		return buf
	},
}

// ClickHouse Chunked protocol frame format (from ClickHouse C++ source ReadBufferFromPocoSocketChunked.h):
//
//   Chunk begins   | [4 bytes LE: chunk_size]   ← uint32 little-endian, frame data length
//                  |   packet_type + packet_data
//                  |   ... may contain multiple packets ...
//   Chunk ends     | [4 bytes: 0x00000000]      ← 4-byte zero end marker
//
// Three modes:
//   Basic:      single frame contains one packet
//   Datastream: single frame contains multiple consecutive packets
//   Multipart:  packet data spans multiple frames (chunk parts)

const (
	// chunkedHeaderSize is the chunk frame header size (4-byte uint32 LE)
	chunkedHeaderSize = 4
	// chunkedEndMarker is the chunk end marker value
	chunkedEndMarker = uint32(0)
	// maxChunkSize is the reasonable maximum chunk size (defensive limit, 256MB)
	maxChunkSize = 256 * 1024 * 1024
	// defaultChunkPayloadSize is the ChunkedWriter's default chunk payload size
	defaultChunkPayloadSize = 64 * 1024
)

// ChunkedReader strips chunked frame headers and end markers from the underlying io.Reader,
// exposing a "raw" protocol data stream to upper layers, requiring no changes to existing packet parsing logic.
//
// Implements io.Reader interface. When enabled=false, directly passes through to underlying Reader.
// Note: ChunkedReader is not concurrency-safe. Callers must ensure single goroutine access.
type ChunkedReader struct {
	r         io.Reader
	enabled   bool
	remaining int // Remaining unread bytes in the current frame
}

// NewChunkedReader creates a ChunkedReader. enabled controls whether chunked deframing is active.
func NewChunkedReader(r io.Reader, enabled bool) *ChunkedReader {
	return &ChunkedReader{
		r:       r,
		enabled: enabled,
	}
}

// Enabled returns whether chunked deframing is currently active.
func (cr *ChunkedReader) Enabled() bool {
	return cr.enabled
}

// Read implements io.Reader.
// When enabled=true, automatically strips chunk frame headers and end markers.
// When enabled=false, directly passes through to underlying Reader.
func (cr *ChunkedReader) Read(p []byte) (int, error) {
	if !cr.enabled {
		return cr.r.Read(p)
	}

	for {
		if cr.remaining > 0 {
			// Current frame still has data to read
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

		// Need to read the next chunk header
		size, err := cr.readChunkHeader()
		if err != nil {
			return 0, err // io.EOF will be correctly propagated
		}

		if size == 0 {
			// Chunk end marker (0x00000000); continue reading the next chunk
			continue
		}

		cr.remaining = int(size)
		// Return to top of loop, read data from new frame
	}
}

// readChunkHeader reads the 4-byte chunk frame header and returns the chunk size.
// If the underlying Reader has reached EOF, returns (0, io.EOF).
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

// ChunkedWriter wraps written data in chunked frame format before sending to the underlying Writer.
//
// Each Write call produces one chunk: [4 bytes LE: size][data][4 bytes: 0x00000000]
// When enabled=false, directly passes through to underlying Writer.
// Note: ChunkedWriter is not concurrency-safe. Callers must ensure single goroutine access.
type ChunkedWriter struct {
	w       io.Writer
	enabled bool
}

// NewChunkedWriter creates a ChunkedWriter. enabled controls whether chunked framing is active.
func NewChunkedWriter(w io.Writer, enabled bool) *ChunkedWriter {
	return &ChunkedWriter{
		w:       w,
		enabled: enabled,
	}
}

// Enabled returns whether chunked framing is currently active.
func (cw *ChunkedWriter) Enabled() bool {
	return cw.enabled
}

// Write implements io.Writer.
// When enabled=true, wraps data in a chunk frame: [size: 4 bytes LE][data][end: 4 bytes 0x00]
// When enabled=false, directly passes through to underlying Writer.
// R2-3: When data exceeds defaultChunkPayloadSize, split into multiple chunks,
// aligned with ClickHouse WriteBufferFromPocoSocketChunked multipart mode behavior.
// P2 #8: Use sync.Pool to cache frame buffers, reducing memory allocations for high-frequency small packet writes.
func (cw *ChunkedWriter) Write(p []byte) (int, error) {
	if !cw.enabled {
		return cw.w.Write(p)
	}
	if len(p) == 0 {
		return 0, nil
	}

	totalWritten := 0
	for len(p) > 0 {
		// R2-3: Fragmentation — each chunk has max size of defaultChunkPayloadSize
		chunkSize := len(p)
		if chunkSize > defaultChunkPayloadSize {
			chunkSize = defaultChunkPayloadSize
		}
		chunk := p[:chunkSize]
		p = p[chunkSize:]

		// Merge into a single write: [header:4][data:N][endMarker:4]
		frameSize := chunkedHeaderSize + chunkSize + chunkedHeaderSize
		frame := chunkedFramePool.Get().([]byte)
		if cap(frame) < frameSize {
			frame = make([]byte, frameSize)
		} else {
			frame = frame[:frameSize]
		}
		binary.LittleEndian.PutUint32(frame[:chunkedHeaderSize], uint32(chunkSize))
		copy(frame[chunkedHeaderSize:], chunk)
		// endMarker is at frame[chunkedHeaderSize+chunkSize:]
		binary.LittleEndian.PutUint32(frame[chunkedHeaderSize+chunkSize:], 0)

		_, err := cw.w.Write(frame)

		// Return to pool (oversized frames are not returned to avoid accumulating large buffers in the pool)
		// R4-6: Return even on write failure — buffer is re-sliced via frame[:frameSize] on next use,
		// old data will be completely overwritten, no risk of dirty data residue.
		const maxPoolFrameSize = 256 * 1024 // 256KB
		if cap(frame) <= maxPoolFrameSize {
			chunkedFramePool.Put(frame)
		}

		if err != nil {
			return totalWritten, fmt.Errorf("chunked: write frame: %w", err)
		}

		totalWritten += chunkSize
	}

	return totalWritten, nil
}
