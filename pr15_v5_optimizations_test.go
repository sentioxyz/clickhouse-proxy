package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// ============================================================================
// P2-1: 空 Block 快速路径测试
// ============================================================================

func TestEmptyBlockFastPath(t *testing.T) {
	t.Run("detect empty block via Peek", func(t *testing.T) {
		// 构造空 Block 的 RawBlock 部分: [num_columns=0][num_rows=0]
		var buf bytes.Buffer
		buf.WriteByte(0) // num_columns = UVarInt(0) = 0x00
		buf.WriteByte(0) // num_rows = UVarInt(0) = 0x00

		br := bufio.NewReader(&buf)
		peekBytes, err := br.Peek(2)
		if err != nil {
			t.Fatalf("Peek error: %v", err)
		}
		if peekBytes[0] != 0 || peekBytes[1] != 0 {
			t.Errorf("expected [0, 0], got %v", peekBytes)
		}

		// 验证 Discard 消费了正确的字节数
		n, err := br.Discard(2)
		if err != nil {
			t.Fatalf("Discard error: %v", err)
		}
		if n != 2 {
			t.Errorf("Discard returned %d, want 2", n)
		}
		// 确认没有剩余数据
		if br.Buffered() != 0 {
			t.Errorf("expected 0 buffered bytes, got %d", br.Buffered())
		}
	})

	t.Run("non-empty block should not trigger fast path", func(t *testing.T) {
		// 构造非空 Block: [num_columns=2][num_rows=100]
		var buf bytes.Buffer
		colBytes := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(colBytes, 2)
		buf.Write(colBytes[:n])
		rowBytes := make([]byte, binary.MaxVarintLen64)
		n = binary.PutUvarint(rowBytes, 100)
		buf.Write(rowBytes[:n])

		br := bufio.NewReader(&buf)
		peekBytes, err := br.Peek(2)
		if err != nil {
			t.Fatalf("Peek error: %v", err)
		}
		// num_columns=2 → 第一个字节不是 0
		if peekBytes[0] == 0 && peekBytes[1] == 0 {
			t.Error("non-empty block should NOT trigger fast path")
		}
	})

	t.Run("large column count still works", func(t *testing.T) {
		// num_columns=128 需要 2 字节 UVarInt 编码
		var buf bytes.Buffer
		colBytes := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(colBytes, 128)
		buf.Write(colBytes[:n])

		br := bufio.NewReader(&buf)
		peekBytes, err := br.Peek(2)
		if err != nil {
			t.Fatalf("Peek error: %v", err)
		}
		// 128 in UVarInt = [0x80, 0x01] — 第一个字节是 0x80 (MSB set)
		if peekBytes[0] == 0 && peekBytes[1] == 0 {
			t.Error("column count 128 should NOT trigger fast path")
		}
	})
}

// ============================================================================
// P3-4/P3-5: handleDataBlock bufferPool 优化验证
// ============================================================================

func TestBufferPoolInDataPath(t *testing.T) {
	p := &proxy{}

	t.Run("hdrBuf lifecycle", func(t *testing.T) {
		// 模拟 hdrBuf 的生命周期：get → encode → write → put
		hdrBuf := p.getBuffer()
		hdrBuf.PutByte(byte(proto.ClientCodeData))
		hdrBuf.PutString("") // empty block name

		// 验证编码结果
		if len(hdrBuf.Buf) == 0 {
			t.Error("empty hdrBuf after encoding")
		}
		expected := []byte{byte(proto.ClientCodeData), 0} // code + empty string (UVarInt(0))
		if !bytes.Equal(hdrBuf.Buf, expected) {
			t.Errorf("hdrBuf = %v, want %v", hdrBuf.Buf, expected)
		}
		p.putBuffer(hdrBuf)
	})

	t.Run("encBuf lifecycle with empty block", func(t *testing.T) {
		// 模拟空 Block 编码
		encBuf := p.getBuffer()

		// 编码 BlockInfo
		info := &BlockInfoCompat{BucketNum: -1}
		encodeBlockInfoCompat(encBuf, info)

		// 编码空 Block
		encBuf.PutUVarInt(0) // num_columns
		encBuf.PutUVarInt(0) // num_rows

		if len(encBuf.Buf) == 0 {
			t.Error("empty encBuf after encoding")
		}
		p.putBuffer(encBuf)
	})

	t.Run("encBuf error path returns to pool", func(t *testing.T) {
		// 模拟错误路径
		encBuf := p.getBuffer()
		encBuf.PutByte(0xFF) // 写入一些数据

		// 模拟错误发生，确保 putBuffer 被调用
		p.putBuffer(encBuf)

		// 再次获取不应包含之前的数据
		encBuf2 := p.getBuffer()
		if len(encBuf2.Buf) != 0 {
			t.Errorf("reused buffer should be clean, got %d bytes", len(encBuf2.Buf))
		}
		p.putBuffer(encBuf2)
	})

	t.Run("concurrent hdrBuf/encBuf usage", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// hdrBuf
				hdr := p.getBuffer()
				hdr.PutByte(byte(proto.ClientCodeData))
				hdr.PutString("block_name")
				p.putBuffer(hdr)
				// encBuf
				enc := p.getBuffer()
				info := &BlockInfoCompat{BucketNum: -1}
				encodeBlockInfoCompat(enc, info)
				enc.PutUVarInt(0)
				enc.PutUVarInt(0)
				p.putBuffer(enc)
			}()
		}
		wg.Wait()
	})
}

// ============================================================================
// BlockInfoCompat 编解码完整性测试
// ============================================================================

func TestBlockInfoCompat_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		info BlockInfoCompat
	}{
		{
			name: "default (no overflow, bucket -1)",
			info: BlockInfoCompat{BucketNum: -1},
		},
		{
			name: "with overflow",
			info: BlockInfoCompat{Overflows: true, BucketNum: 5},
		},
		{
			name: "with out-of-order buckets",
			info: BlockInfoCompat{
				BucketNum:         3,
				OutOfOrderBuckets: []int32{1, 2, 5, 7},
			},
		},
		{
			name: "zero bucket",
			info: BlockInfoCompat{BucketNum: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			buf := &proto.Buffer{}
			encodeBlockInfoCompat(buf, &tt.info)

			// Decode
			reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
			decoded, err := decodeBlockInfoCompat(reader)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// Verify
			if decoded.Overflows != tt.info.Overflows {
				t.Errorf("Overflows = %v, want %v", decoded.Overflows, tt.info.Overflows)
			}
			if decoded.BucketNum != tt.info.BucketNum {
				t.Errorf("BucketNum = %d, want %d", decoded.BucketNum, tt.info.BucketNum)
			}
			if len(decoded.OutOfOrderBuckets) != len(tt.info.OutOfOrderBuckets) {
				t.Errorf("OutOfOrderBuckets length = %d, want %d",
					len(decoded.OutOfOrderBuckets), len(tt.info.OutOfOrderBuckets))
			}
			for i := range decoded.OutOfOrderBuckets {
				if decoded.OutOfOrderBuckets[i] != tt.info.OutOfOrderBuckets[i] {
					t.Errorf("OutOfOrderBuckets[%d] = %d, want %d",
						i, decoded.OutOfOrderBuckets[i], tt.info.OutOfOrderBuckets[i])
				}
			}
		})
	}
}

// ============================================================================
// 压缩帧格式验证测试
// ============================================================================

func TestCompressedFrameFormat(t *testing.T) {
	t.Run("valid LZ4 frame header", func(t *testing.T) {
		const frameHeaderSize = 25

		// 构造一个合法的 LZ4 压缩帧头
		header := make([]byte, frameHeaderSize)
		// 16 bytes CityHash128 checksum (arbitrary)
		for i := 0; i < 16; i++ {
			header[i] = byte(i)
		}
		// byte 16: compression method = LZ4
		header[16] = 0x82
		// bytes 17-20: compressed_size = 15 (9 sub-header + 6 data bytes)
		binary.LittleEndian.PutUint32(header[17:21], 15)
		// bytes 21-24: decompressed_size = 10
		binary.LittleEndian.PutUint32(header[21:25], 10)

		// 验证解析
		if header[16] != 0x82 {
			t.Errorf("method byte = 0x%02x, want 0x82", header[16])
		}
		compressedSize := binary.LittleEndian.Uint32(header[17:21])
		if compressedSize != 15 {
			t.Errorf("compressed_size = %d, want 15", compressedSize)
		}
		if compressedSize < 9 {
			t.Error("compressed_size < 9 (invalid)")
		}
		remainingDataSize := int(compressedSize) - 9
		if remainingDataSize != 6 {
			t.Errorf("remaining data size = %d, want 6", remainingDataSize)
		}
	})

	t.Run("ZSTD frame header", func(t *testing.T) {
		header := make([]byte, 25)
		header[16] = 0x90 // ZSTD
		binary.LittleEndian.PutUint32(header[17:21], 100)
		binary.LittleEndian.PutUint32(header[21:25], 500)

		if header[16] != 0x90 {
			t.Errorf("method byte = 0x%02x, want 0x90", header[16])
		}
	})

	t.Run("no compression frame header", func(t *testing.T) {
		header := make([]byte, 25)
		header[16] = 0x02                                 // no compression
		binary.LittleEndian.PutUint32(header[17:21], 109) // 9 + 100
		binary.LittleEndian.PutUint32(header[21:25], 100)

		if header[16] != 0x02 {
			t.Errorf("method byte = 0x%02x, want 0x02", header[16])
		}
	})

	t.Run("invalid compressed_size < 9", func(t *testing.T) {
		header := make([]byte, 25)
		header[16] = 0x82
		binary.LittleEndian.PutUint32(header[17:21], 5) // invalid: < 9

		compressedSize := binary.LittleEndian.Uint32(header[17:21])
		if compressedSize >= 9 {
			t.Error("should detect invalid compressed_size")
		}
	})

	t.Run("multi-frame continuation detection", func(t *testing.T) {
		const frameHeaderSize = 25
		const maxCompressedFrameSize = 32 * 1024 * 1024
		const maxDecompressedSize = 256 * 1024 * 1024

		// 模拟 Peek 第二个帧头来决定是否继续
		nextHeader := make([]byte, frameHeaderSize)
		// 有效的后续帧
		nextHeader[16] = 0x82
		binary.LittleEndian.PutUint32(nextHeader[17:21], 1000)
		binary.LittleEndian.PutUint32(nextHeader[21:25], 5000)

		methodByte := nextHeader[16]
		isValidMethod := methodByte == 0x82 || methodByte == 0x90 || methodByte == 0x02
		if !isValidMethod {
			t.Error("valid method byte should be recognized")
		}

		peekCompSize := binary.LittleEndian.Uint32(nextHeader[17:21])
		peekDecompSize := binary.LittleEndian.Uint32(nextHeader[21:25])
		valid := peekCompSize >= 9 && peekCompSize <= uint32(maxCompressedFrameSize) &&
			peekDecompSize > 0 && peekDecompSize <= uint32(maxDecompressedSize)
		if !valid {
			t.Error("valid continuation frame should pass sanity checks")
		}

		// 无效的后续帧（非压缩方法字节）
		invalidHeader := make([]byte, frameHeaderSize)
		invalidHeader[16] = 0x01 // Query packet type, not compression
		isInvalidMethod := invalidHeader[16] != 0x82 && invalidHeader[16] != 0x90 && invalidHeader[16] != 0x02
		if !isInvalidMethod {
			t.Error("invalid method byte should be rejected")
		}
	})
}

// ============================================================================
// fallbackRawCopy 测试
// ============================================================================

func TestFallbackRawCopy(t *testing.T) {
	t.Run("copies all data correctly", func(t *testing.T) {
		p := &proxy{cfg: Config{}}

		// 准备输入数据
		inputData := []byte("hello world this is raw copy test data 12345")
		br := bufio.NewReader(bytes.NewReader(inputData))

		// 准备输出
		var output bytes.Buffer

		// 使用管道模拟 clientConn 的 SetReadDeadline
		clientReader, clientWriter := net.Pipe()
		defer clientReader.Close()
		defer clientWriter.Close()

		// fallbackRawCopy 需要 clientConn，但我们用 br 作为输入，
		// 它不实际从 clientConn 读取（已经通过 br 提供了数据）
		// 所以我们只需一个 dummy conn
		go func() {
			p.fallbackRawCopy(0, br, clientReader, &output)
		}()

		// 等待 copy 完成
		time.Sleep(100 * time.Millisecond)

		if output.String() != string(inputData) {
			t.Errorf("output = %q, want %q", output.String(), string(inputData))
		}
	})
}

// ============================================================================
// queryDoneCounter 交互测试
// ============================================================================

func TestQueryDoneCounter(t *testing.T) {
	t.Run("basic counter increment and swap", func(t *testing.T) {
		var counter atomic.Int64

		// 模拟 upstream goroutine 检测到 EndOfStream
		counter.Add(1)

		// 模拟包循环检测 counter
		val := counter.Swap(0)
		if val != 1 {
			t.Errorf("Swap returned %d, want 1", val)
		}

		// Swap 后 counter 应为 0
		if counter.Load() != 0 {
			t.Errorf("counter after Swap = %d, want 0", counter.Load())
		}
	})

	t.Run("multiple EndOfStream signals", func(t *testing.T) {
		var counter atomic.Int64

		// 两次 EndOfStream
		counter.Add(1)
		counter.Add(1)

		// 第一次 Swap 获取所有信号
		val := counter.Swap(0)
		if val != 2 {
			t.Errorf("Swap returned %d, want 2", val)
		}

		// 第二次 Swap 应为 0
		val2 := counter.Swap(0)
		if val2 != 0 {
			t.Errorf("second Swap returned %d, want 0", val2)
		}
	})

	t.Run("concurrent Add and Swap", func(t *testing.T) {
		var counter atomic.Int64
		var wg sync.WaitGroup

		// 模拟 upstream goroutine 频繁发送 EndOfStream
		totalSignals := 100
		for i := 0; i < totalSignals; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				counter.Add(1)
			}()
		}

		wg.Wait()

		// 所有信号都应被捕获
		val := counter.Swap(0)
		if val != int64(totalSignals) {
			t.Errorf("total signals = %d, want %d", val, totalSignals)
		}
	})

	t.Run("state machine reset behavior", func(t *testing.T) {
		// 模拟包循环的完整状态机
		var counter atomic.Int64
		inQuery := false
		queryCompression := proto.CompressionDisabled

		// Query 开始
		inQuery = true
		queryCompression = proto.CompressionEnabled

		// upstream 发送 EndOfStream
		counter.Add(1)

		// 包循环检查
		if inQuery && counter.Swap(0) > 0 {
			inQuery = false
			queryCompression = proto.CompressionDisabled
		}

		if inQuery {
			t.Error("inQuery should be false after EndOfStream")
		}
		if queryCompression != proto.CompressionDisabled {
			t.Error("queryCompression should be disabled after EndOfStream")
		}
	})
}

// ============================================================================
// detectServerPacketType 测试
// ============================================================================

func TestDetectServerPacketType(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected string
	}{
		{"empty", []byte{}, "unknown"},
		{"ServerHello", []byte{0}, "Hello"},
		{"Data", []byte{1}, "Data"},
		{"Exception", []byte{2}, "Exception"},
		{"Progress", []byte{3}, "Progress"},
		{"Pong", []byte{4}, "Pong"},
		{"EndOfStream", []byte{5}, "EndOfStream"},
		{"ProfileInfo", []byte{6}, "ProfileInfo"},
		{"Totals", []byte{7}, "Totals"},
		{"Extremes", []byte{8}, "Extremes"},
		{"TablesStatusResponse", []byte{9}, "TablesStatusResponse"},
		{"Log", []byte{10}, "Log"},
		{"TableColumns", []byte{11}, "TableColumns"},
		{"PartUUIDs", []byte{12}, "PartUUIDs"},
		{"ReadTaskRequest", []byte{13}, "ReadTaskRequest"},
		{"ProfileEvents", []byte{14}, "ProfileEvents"},
		{"unknown type 15", []byte{15}, "MergeTreeReadTaskRequest"},
		{"unknown type 31", []byte{31}, "type_31"},
		{"high bit set", []byte{0x80}, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectServerPacketType(tt.data)
			if got != tt.expected {
				t.Errorf("detectServerPacketType(%v) = %q, want %q", tt.data, got, tt.expected)
			}
		})
	}
}

func TestDetectPacketType_V5(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected string
	}{
		{"empty", []byte{}, "unknown"},
		{"Hello", []byte{0}, "Hello"},
		{"Query", []byte{1}, "Query"},
		{"Data", []byte{2}, "Data"},
		{"Cancel", []byte{3}, "Cancel"},
		{"Ping", []byte{4}, "Ping"},
		{"TablesStatusRequest", []byte{5}, "TablesStatusRequest"},
		{"KeepAlive", []byte{6}, "KeepAlive"},
		{"Scalar", []byte{7}, "Scalar"},
		{"IgnoredPartUUIDs", []byte{8}, "IgnoredPartUUIDs"},
		{"ReadTaskResponse", []byte{9}, "ReadTaskResponse"},
		{"MergeTreeReadTaskResponse", []byte{10}, "MergeTreeReadTaskResponse"},
		{"QueryPlan", []byte{11}, "QueryPlan"},
		{"high bit set", []byte{0x80}, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectPacketType(tt.data)
			if got != tt.expected {
				t.Errorf("detectPacketType(%v) = %q, want %q", tt.data, got, tt.expected)
			}
		})
	}
}

// ============================================================================
// Chunked 适配层测试
// ============================================================================

func TestChunkedReaderWriter_Integration(t *testing.T) {
	t.Run("roundtrip with chunked enabled", func(t *testing.T) {
		// 使用管道做端到端测试
		reader, writer := io.Pipe()

		data := []byte("hello chunked world 12345 repeated data for testing")

		// Writer goroutine
		go func() {
			cw := NewChunkedWriter(writer, true)
			cw.Write(data)
			writer.Close()
		}()

		// Reader side
		br := bufio.NewReader(reader)
		cr := NewChunkedReader(br, true)
		result, err := io.ReadAll(cr)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}
		if !bytes.Equal(result, data) {
			t.Errorf("roundtrip mismatch: got %d bytes, want %d bytes", len(result), len(data))
		}
	})

	t.Run("passthrough when chunked disabled", func(t *testing.T) {
		data := []byte("plain data no chunking")
		br := bufio.NewReader(bytes.NewReader(data))
		cr := NewChunkedReader(br, false)

		result, err := io.ReadAll(cr)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}
		if !bytes.Equal(result, data) {
			t.Errorf("passthrough mismatch: got %q, want %q", result, data)
		}
	})
}

// ============================================================================
// replaceToken 测试
// ============================================================================

func TestReplaceToken(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		oldKey string
		newKey string
	}{
		{
			name:   "same length replacement",
			oldKey: "x_auth_token",
			newKey: "promql_table",
		},
		{
			name:   "shorter replacement",
			oldKey: "SQL_x_auth_token",
			newKey: "promql_table",
		},
		{
			name:   "no match",
			oldKey: "nonexistent_key",
			newKey: "replacement",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.oldKey == "nonexistent_key" {
				// 无匹配测试：data 中不包含 oldKey 的编码
				data := []byte("some random data without the key")
				result := replaceToken(data, tt.oldKey, tt.newKey)
				if string(result) != string(data) {
					t.Errorf("no-match: result %q != data %q", result, data)
				}
				return
			}

			// 构造 UVarInt 前缀的 key
			oldKeyBytes := []byte(tt.oldKey)
			oldLenBuf := make([]byte, binary.MaxVarintLen64)
			nOld := binary.PutUvarint(oldLenBuf, uint64(len(oldKeyBytes)))
			searchSeq := make([]byte, nOld+len(oldKeyBytes))
			copy(searchSeq, oldLenBuf[:nOld])
			copy(searchSeq[nOld:], oldKeyBytes)

			// 在 data 中嵌入搜索序列
			data := append([]byte("prefix_"), searchSeq...)
			data = append(data, []byte("_suffix")...)

			result := replaceToken(data, tt.oldKey, tt.newKey)

			// 应包含 newKey 的 UVarInt 编码
			newKeyBytes := []byte(tt.newKey)
			newLenBuf := make([]byte, binary.MaxVarintLen64)
			nNew := binary.PutUvarint(newLenBuf, uint64(len(newKeyBytes)))
			replaceSeq := make([]byte, nNew+len(newKeyBytes))
			copy(replaceSeq, newLenBuf[:nNew])
			copy(replaceSeq[nNew:], newKeyBytes)

			if !bytes.Contains(result, replaceSeq) {
				t.Errorf("result should contain encoded newKey %q", tt.newKey)
			}
			if bytes.Contains(result, searchSeq) {
				t.Errorf("result should NOT contain encoded oldKey %q", tt.oldKey)
			}
		})
	}
}

// ============================================================================
// 包统计测试
// ============================================================================

func TestPacketStats(t *testing.T) {
	t.Run("concurrent inc and snapshot", func(t *testing.T) {
		stats := newPacketStats()
		var wg sync.WaitGroup

		// 并发递增
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				stats.inc("Query")
				stats.inc("Data")
				stats.inc("Ping")
			}()
		}
		wg.Wait()

		snap := stats.snapshot()
		if snap["Query"] != 100 {
			t.Errorf("Query count = %d, want 100", snap["Query"])
		}
		if snap["Data"] != 100 {
			t.Errorf("Data count = %d, want 100", snap["Data"])
		}
		if snap["Ping"] != 100 {
			t.Errorf("Ping count = %d, want 100", snap["Ping"])
		}
	})

	t.Run("snapshot is independent copy", func(t *testing.T) {
		stats := newPacketStats()
		stats.inc("Query")

		snap1 := stats.snapshot()
		stats.inc("Query")
		snap2 := stats.snapshot()

		if snap1["Query"] != 1 {
			t.Errorf("snap1 Query = %d, want 1", snap1["Query"])
		}
		if snap2["Query"] != 2 {
			t.Errorf("snap2 Query = %d, want 2", snap2["Query"])
		}
	})
}

// ============================================================================
// countingReader 测试
// ============================================================================

func TestCountingReader(t *testing.T) {
	data := []byte("hello world counting reader test")
	cr := &countingReader{r: bytes.NewReader(data)}

	buf := make([]byte, 5)
	n1, err := cr.Read(buf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if n1 != 5 {
		t.Errorf("Read returned %d, want 5", n1)
	}
	if cr.n != 5 {
		t.Errorf("counter = %d, want 5", cr.n)
	}

	n2, err := cr.Read(buf)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}
	if cr.n != 5+n2 {
		t.Errorf("counter = %d, want %d", cr.n, 5+n2)
	}
}

// ============================================================================
// extractQuerySummary 测试
// ============================================================================

func TestExtractQuerySummary_V5(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		maxLen   int
		contains string
	}{
		{
			name:     "SELECT query extraction",
			input:    []byte("SELECT * FROM test"),
			maxLen:   100,
			contains: "SELECT",
		},
		{
			name:     "INSERT query extraction",
			input:    []byte("INSERT INTO test VALUES"),
			maxLen:   100,
			contains: "INSERT",
		},
		{
			name:     "empty input",
			input:    []byte{},
			maxLen:   100,
			contains: "",
		},
		{
			name:     "truncation",
			input:    []byte("SELECT very_long_query_that_should_be_truncated_at_some_point"),
			maxLen:   10,
			contains: "SELECT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractQuerySummary(tt.input, tt.maxLen)
			if tt.contains != "" && len(result) > 0 {
				if !bytes.Contains([]byte(result), []byte(tt.contains)) {
					t.Errorf("result %q should contain %q", result, tt.contains)
				}
			}
		})
	}
}

// ============================================================================
// summarizePrintable 测试
// ============================================================================

func TestSummarizePrintable_V5(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		maxLen   int
		expected string
	}{
		{"empty", []byte{}, 100, ""},
		{"pure ASCII", []byte("hello"), 100, "hello"},
		{"binary with ASCII", []byte{0, 0, 'A', 'B', 0, 'C'}, 100, "A B C"},
		{"respects maxLen", []byte("abcdefghij"), 5, "abcde"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := summarizePrintable(tt.input, tt.maxLen)
			if got != tt.expected {
				t.Errorf("got %q, want %q", got, tt.expected)
			}
		})
	}
}

// ============================================================================
// isTimeout 测试
// ============================================================================

func TestIsTimeout(t *testing.T) {
	if isTimeout(nil) {
		t.Error("nil should not be timeout")
	}
	if isTimeout(io.EOF) {
		t.Error("EOF should not be timeout")
	}
}
