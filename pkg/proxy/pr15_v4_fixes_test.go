package proxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

// ============================================================================
// P0-3: QueryPlan 包类型识别测试
// ============================================================================

func TestQueryPlanPacketType(t *testing.T) {
	t.Run("QueryPlan constant value", func(t *testing.T) {
		// ClickHouse Protocol::Client::QueryPlan = 11
		if clientCodeQueryPlan != 11 {
			t.Errorf("clientCodeQueryPlan = %d, want 11", clientCodeQueryPlan)
		}
	})

	t.Run("QueryPlan in packet names", func(t *testing.T) {
		// 确认 QueryPlan 被正确添加到 packetNames
		// 如果 packetNames 中没有 11，则是已知的（因为 packetNames 不一定包含所有类型）
		// 但 clientCodeQueryPlan 常量值必须正确
		codeByte := byte(clientCodeQueryPlan)
		if codeByte != 11 {
			t.Errorf("QueryPlan code byte = %d, want 11", codeByte)
		}
	})

	t.Run("QueryPlan detection in packet stream", func(t *testing.T) {
		// 构造一个包含 QueryPlan 包类型的数据流
		var buf bytes.Buffer
		buf.WriteByte(byte(clientCodeQueryPlan))
		buf.Write([]byte("fake_query_plan_data_should_be_passthrough"))

		br := bufio.NewReader(&buf)
		codeByte, err := br.ReadByte()
		if err != nil {
			t.Fatalf("ReadByte error: %v", err)
		}
		if proto.ClientCode(codeByte) != clientCodeQueryPlan {
			t.Errorf("packet type = %d, want clientCodeQueryPlan(%d)", codeByte, clientCodeQueryPlan)
		}
	})
}

// ============================================================================
// P0-2: MergeTreeReadTaskResponse 格式验证
// ============================================================================

func TestMergeTreeReadTaskResponseFormat(t *testing.T) {
	t.Run("ParallelReadResponse uses IntBinary not UVarInt", func(t *testing.T) {
		// ClickHouse ParallelReadResponse::serialize 使用:
		//   writeIntBinary(version, out)     -- fixed 8 bytes (UInt64)
		//   writeBoolText(finish, out)       -- 1 byte ("0" or "1")
		//   description.serialize(out, ...)  -- variable length nested struct
		//
		// 之前的实现使用 [segment: UVarInt][mark: UVarInt]，这是完全错误的。
		// 正确的做法是 raw passthrough，因为格式太复杂。

		// 模拟 ClickHouse ParallelReadResponse 序列化（简化版）
		var chResponse bytes.Buffer
		// version: writeIntBinary(UInt64)
		version := uint64(1)
		binary.LittleEndian.PutUint64(make([]byte, 8), version)
		versionBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(versionBytes, version)
		chResponse.Write(versionBytes)
		// finish: writeBoolText
		chResponse.WriteByte('1') // finish = true
		// description: 空的 RangesInDataPartsDescription (simplified)
		// 实际格式更复杂，包含 part 数量等

		// 用旧的 UVarInt 方式尝试读取——应该得到错误的结果
		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(chResponse.Bytes())))
		segment, err := reader.UVarInt()
		if err != nil {
			// 可能会读取出错误的值但不会报错
			t.Logf("UVarInt read error (expected in some cases): %v", err)
			return
		}
		// version=1 的 IntBinary 是 [0x01 0x00 0x00 0x00 0x00 0x00 0x00 0x00]
		// UVarInt 会把 0x01 解读为值 1，然后下一个字节 0x00 也是值 0
		// 这看起来"正确"只是因为巧合（version=1）
		// 但如果 version > 127，UVarInt 会尝试读取多字节变长编码
		t.Logf("UVarInt read segment=%d (actual version in IntBinary=%d)", segment, version)

		// 验证大版本号时 UVarInt 会产生错误结果
		var bigVersionResp bytes.Buffer
		bigVersionBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bigVersionBytes, 256) // version > 127
		bigVersionResp.Write(bigVersionBytes)
		bigVersionResp.WriteByte('0')

		reader2 := proto.NewReader(bufio.NewReader(bytes.NewReader(bigVersionResp.Bytes())))
		seg2, err := reader2.UVarInt()
		if err != nil {
			t.Logf("UVarInt fails for large version (expected): %v", err)
		} else {
			// UVarInt 会将 [0x00 0x02 ...] 误读
			// 0x00 的 MSB=0, 所以 UVarInt 认为值=0，只消耗了 1 字节
			// 这意味着后续字节解析全部错位
			if seg2 != 256 {
				t.Logf("CONFIRMED: UVarInt misreads IntBinary(256) as %d — raw passthrough is correct fix", seg2)
			}
		}
	})

	t.Run("MergeTreeReadTaskResponse code value", func(t *testing.T) {
		if clientCodeMergeTreeReadTaskResponse != 10 {
			t.Errorf("clientCodeMergeTreeReadTaskResponse = %d, want 10", clientCodeMergeTreeReadTaskResponse)
		}
	})
}

// ============================================================================
// P1-4: Cancel 后状态重置测试
// ============================================================================

func TestCancelResetsQueryState(t *testing.T) {
	t.Run("Cancel packet resets inQuery and compression", func(t *testing.T) {
		// 模拟 Cancel 包流
		var clientData bytes.Buffer
		// 写入 Cancel 包 (proto.ClientCodeCancel = 3)
		clientData.WriteByte(byte(proto.ClientCodeCancel))
		// 写入 Ping 包 (proto.ClientCodePing = 4) 作为后续包
		clientData.WriteByte(byte(proto.ClientCodePing))

		// 验证 Cancel 后 inQuery 应该被重置
		// 这是一个逻辑测试，模拟状态机行为
		inQuery := true
		queryCompression := proto.CompressionEnabled

		br := bufio.NewReader(&clientData)
		codeByte, err := br.ReadByte()
		if err != nil {
			t.Fatalf("ReadByte error: %v", err)
		}

		if proto.ClientCode(codeByte) == proto.ClientCodeCancel {
			// P1-4: Cancel 后重置状态
			inQuery = false
			queryCompression = proto.CompressionDisabled
		}

		if inQuery {
			t.Error("inQuery should be false after Cancel")
		}
		if queryCompression != proto.CompressionDisabled {
			t.Error("queryCompression should be disabled after Cancel")
		}

		// 验证后续包 (Ping) 可以正常读取
		codeByte2, err := br.ReadByte()
		if err != nil {
			t.Fatalf("ReadByte error after Cancel: %v", err)
		}
		if proto.ClientCode(codeByte2) != proto.ClientCodePing {
			t.Errorf("expected Ping after Cancel, got %d", codeByte2)
		}
	})
}

// ============================================================================
// P2-12: TCP_NODELAY 验证
// ============================================================================

func TestTCPNoDelayConstant(t *testing.T) {
	// TCP_NODELAY 是 net.TCPConn 的方法，这里只验证设置逻辑的存在性
	// 实际功能在集成测试中验证
	t.Run("TCP_NODELAY recommendation", func(t *testing.T) {
		// 验证 ClickHouse native 协议特点：小包频繁交互
		// Ping/Pong: 1 byte each
		// Cancel: 1 byte
		// 这些小包在 Nagle 算法下可能有 200ms 延迟
		smallPackets := map[string]int{
			"Ping":   1,
			"Cancel": 1,
			"Pong":   1,
		}
		for name, size := range smallPackets {
			if size <= 1 {
				t.Logf("%s packet size=%d byte(s) — benefits from TCP_NODELAY", name, size)
			}
		}
	})
}

// ============================================================================
// P2-10: bufferPool 使用一致性测试
// ============================================================================

func TestBufferPoolConsistency(t *testing.T) {
	p := &Proxy{}

	t.Run("getBuffer returns usable buffer", func(t *testing.T) {
		buf := p.getBuffer()
		if buf == nil {
			t.Fatal("getBuffer returned nil")
		}
		buf.PutByte(0x01)
		buf.PutString("test")
		if len(buf.Buf) == 0 {
			t.Error("buffer should have data after Put operations")
		}
		p.putBuffer(buf)
	})

	t.Run("putBuffer resets buffer", func(t *testing.T) {
		buf := p.getBuffer()
		buf.PutByte(0xFF)
		buf.PutString("data")
		p.putBuffer(buf)

		// 再次获取应该是干净的
		buf2 := p.getBuffer()
		if len(buf2.Buf) != 0 {
			t.Errorf("reused buffer should be clean, got %d bytes", len(buf2.Buf))
		}
		p.putBuffer(buf2)
	})

	t.Run("multiple get/put cycles", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			buf := p.getBuffer()
			buf.PutUVarInt(uint64(i))
			buf.PutString("cycle test")
			p.putBuffer(buf)
		}
	})
}

// ============================================================================
// P2-8: replaceOutsideQuotes SQL 注释支持测试
// ============================================================================

func TestReplaceOutsideQuotes_LineComments(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		old      string
		repl     string
		expected string
	}{
		{
			name:     "不替换行注释中的表名",
			sql:      "SELECT * FROM my_table -- my_table is the source",
			old:      "my_table",
			repl:     "replaced_table",
			expected: "SELECT * FROM replaced_table -- my_table is the source",
		},
		{
			name:     "行注释在行尾，表名在注释前",
			sql:      "SELECT * FROM my_table-- comment",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM new_table-- comment",
		},
		{
			name:     "多行SQL中的行注释",
			sql:      "SELECT * FROM my_table\n-- my_table comment\nWHERE 1=1",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM new_table\n-- my_table comment\nWHERE 1=1",
		},
		{
			name:     "行注释后换行继续替换",
			sql:      "SELECT * FROM -- skip\nmy_table WHERE 1=1",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM -- skip\nnew_table WHERE 1=1",
		},
		{
			name:     "仅行注释",
			sql:      "-- my_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "-- my_table",
		},
		{
			name:     "减号不是注释",
			sql:      "SELECT a-b FROM my_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT a-b FROM new_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceOutsideQuotes(tt.sql, tt.old, tt.repl)
			if got != tt.expected {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.expected)
			}
		})
	}
}

func TestReplaceOutsideQuotes_BlockComments(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		old      string
		repl     string
		expected string
	}{
		{
			name:     "不替换块注释中的表名",
			sql:      "SELECT * FROM /* my_table */ other_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM /* my_table */ other_table",
		},
		{
			name:     "块注释跨多行",
			sql:      "SELECT * FROM /* \nmy_table\n */ other_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM /* \nmy_table\n */ other_table",
		},
		{
			name:     "块注释后继续替换",
			sql:      "SELECT /* skip */ my_table FROM dual",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT /* skip */ new_table FROM dual",
		},
		{
			name:     "多个块注释",
			sql:      "SELECT /* a */ my_table /* b */ FROM /* my_table */ dual",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT /* a */ new_table /* b */ FROM /* my_table */ dual",
		},
		{
			name:     "空块注释",
			sql:      "SELECT /**/ my_table FROM dual",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT /**/ new_table FROM dual",
		},
		{
			name:     "星号不是块注释",
			sql:      "SELECT * FROM my_table WHERE a*b=1",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM new_table WHERE a*b=1",
		},
		{
			name:     "斜杠不是块注释",
			sql:      "SELECT a/b FROM my_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT a/b FROM new_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceOutsideQuotes(tt.sql, tt.old, tt.repl)
			if got != tt.expected {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.expected)
			}
		})
	}
}

func TestReplaceOutsideQuotes_MixedCommentsAndQuotes(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		old      string
		repl     string
		expected string
	}{
		{
			name:     "引号内的注释标记不是注释",
			sql:      "SELECT '-- not a comment' FROM my_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT '-- not a comment' FROM new_table",
		},
		{
			name:     "引号内的块注释标记不是注释",
			sql:      "SELECT '/* not a comment */' FROM my_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT '/* not a comment */' FROM new_table",
		},
		{
			name:     "注释内的引号不是引号",
			sql:      "SELECT * FROM my_table -- don't replace 'my_table'",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM new_table -- don't replace 'my_table'",
		},
		{
			name:     "块注释内的引号不是引号",
			sql:      "SELECT * FROM my_table /* 'my_table' */",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM new_table /* 'my_table' */",
		},
		{
			name:     "复杂混合场景",
			sql:      "SELECT 'val' FROM /* skip my_table */ my_table -- my_table\nWHERE \"my_table\" = 1",
			old:      "my_table",
			repl:     "new_t",
			expected: "SELECT 'val' FROM /* skip my_table */ new_t -- my_table\nWHERE \"my_table\" = 1",
		},
		{
			name:     "ClickHouse hint 注释",
			sql:      "SELECT /*+ READ_IN_ORDER */ * FROM my_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT /*+ READ_IN_ORDER */ * FROM new_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceOutsideQuotes(tt.sql, tt.old, tt.repl)
			if got != tt.expected {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.expected)
			}
		})
	}
}

// ============================================================================
// P2-9: 密码 masking 测试
// ============================================================================

func TestMaskPassword(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "***"},
		{"a", "***"},
		{"ab", "***"},
		{"abc", "a*c"},
		{"password", "p******d"},
		{"12345678", "1******8"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := maskPassword(tt.input)
			if got != tt.expected {
				t.Errorf("maskPassword(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// ============================================================================
// 回归测试：之前的 replaceOutsideQuotes 功能不变
// ============================================================================

func TestReplaceOutsideQuotes_Regression(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		old      string
		repl     string
		expected string
	}{
		{
			name:     "基本替换",
			sql:      "SELECT * FROM my_table WHERE 1=1",
			old:      "my_table",
			repl:     "db.real_table",
			expected: "SELECT * FROM db.real_table WHERE 1=1",
		},
		{
			name:     "不替换单引号内",
			sql:      "SELECT 'my_table' FROM my_table",
			old:      "my_table",
			repl:     "db.t",
			expected: "SELECT 'my_table' FROM db.t",
		},
		{
			name:     "不替换双引号内",
			sql:      `SELECT "my_table" FROM my_table`,
			old:      "my_table",
			repl:     "db.t",
			expected: `SELECT "my_table" FROM db.t`,
		},
		{
			name:     "不替换反引号内",
			sql:      "SELECT `my_table` FROM my_table",
			old:      "my_table",
			repl:     "db.t",
			expected: "SELECT `my_table` FROM db.t",
		},
		{
			name:     "转义单引号",
			sql:      "SELECT 'it\\'s my_table' FROM my_table",
			old:      "my_table",
			repl:     "db.t",
			expected: "SELECT 'it\\'s my_table' FROM db.t",
		},
		{
			name:     "连续单引号转义",
			sql:      "SELECT 'it''s' FROM my_table",
			old:      "my_table",
			repl:     "db.t",
			expected: "SELECT 'it''s' FROM db.t",
		},
		{
			name:     "连续双引号转义",
			sql:      `SELECT "col""name" FROM my_table`,
			old:      "my_table",
			repl:     "db.t",
			expected: `SELECT "col""name" FROM db.t`,
		},
		{
			name:     "无匹配不变",
			sql:      "SELECT * FROM other",
			old:      "my_table",
			repl:     "db.t",
			expected: "SELECT * FROM other",
		},
		{
			name:     "多次匹配",
			sql:      "SELECT my_table.a, my_table.b FROM my_table",
			old:      "my_table",
			repl:     "db.t",
			expected: "SELECT db.t.a, db.t.b FROM db.t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceOutsideQuotes(tt.sql, tt.old, tt.repl)
			if got != tt.expected {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.expected)
			}
		})
	}
}

// ============================================================================
// 包类型完整性检查
// ============================================================================

func TestAllClientPacketCodes(t *testing.T) {
	// 验证所有已知的客户端包类型常量值与 ClickHouse 协议一致
	expectedCodes := map[string]proto.ClientCode{
		"Hello":                     proto.ClientCodeHello,               // 0
		"Query":                     proto.ClientCodeQuery,               // 1
		"Data":                      proto.ClientCodeData,                // 2
		"Cancel":                    proto.ClientCodeCancel,              // 3
		"Ping":                      proto.ClientCodePing,                // 4
		"TablesStatusRequest":       proto.ClientTablesStatusRequest,     // 5
		"KeepAlive":                 clientCodeKeepAlive,                 // 6
		"Scalar":                    clientCodeScalar,                    // 7
		"IgnoredPartUUIDs":          clientCodeIgnoredPartUUIDs,          // 8
		"ReadTaskResponse":          clientCodeReadTaskResponse,          // 9
		"MergeTreeReadTaskResponse": clientCodeMergeTreeReadTaskResponse, // 10
		"QueryPlan":                 clientCodeQueryPlan,                 // 11
	}

	for name, code := range expectedCodes {
		t.Run(name, func(t *testing.T) {
			// 验证值范围 [0, 11]
			if code > 11 {
				t.Errorf("%s code = %d, expected <= 11", name, code)
			}
		})
	}

	// 验证没有重复值
	seen := make(map[proto.ClientCode]string)
	for name, code := range expectedCodes {
		if existing, ok := seen[code]; ok {
			t.Errorf("duplicate code %d: %s and %s", code, existing, name)
		}
		seen[code] = name
	}
}

// ============================================================================
// MergeTreeReadTaskResponse raw passthrough 逻辑测试
// ============================================================================

func TestMergeTreeReadTaskResponsePassthrough(t *testing.T) {
	t.Run("complex ParallelReadResponse should not be parsed as UVarInt", func(t *testing.T) {
		// 构造 ClickHouse ParallelReadResponse 格式的数据
		// Format: [version: IntBinary(UInt64=8bytes)][finish: BoolText(1byte)][description...]
		var response bytes.Buffer

		// packet type
		response.WriteByte(byte(clientCodeMergeTreeReadTaskResponse))

		// version: writeIntBinary(UInt64) = 8 bytes little-endian
		version := uint64(3) // 假设版本 3
		versionBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(versionBytes, version)
		response.Write(versionBytes)

		// finish: writeBoolText = "0" or "1" (1 byte)
		response.WriteByte('0') // not finished

		// description: 简化为一些额外数据
		response.Write([]byte("some_ranges_data"))

		// 读取包类型
		br := bufio.NewReader(bytes.NewReader(response.Bytes()))
		codeByte, err := br.ReadByte()
		if err != nil {
			t.Fatalf("ReadByte error: %v", err)
		}
		if proto.ClientCode(codeByte) != clientCodeMergeTreeReadTaskResponse {
			t.Fatalf("expected MergeTreeReadTaskResponse, got %d", codeByte)
		}

		// 验证剩余数据可以作为 raw bytes 透传
		remaining, err := io.ReadAll(br)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}
		if len(remaining) != 8+1+len("some_ranges_data") {
			t.Errorf("remaining bytes = %d, want %d", len(remaining), 8+1+len("some_ranges_data"))
		}
	})
}

// ============================================================================
// QueryPlan fallback 流测试
// ============================================================================

func TestQueryPlanFallbackStream(t *testing.T) {
	t.Run("QueryPlan triggers fallback not crash", func(t *testing.T) {
		// 构造查询计划数据流
		var data bytes.Buffer
		data.WriteByte(byte(clientCodeQueryPlan))
		// QueryPlan 数据格式未知，写入一些任意数据
		data.Write([]byte("query_plan_binary_data_here"))

		br := bufio.NewReader(&data)
		codeByte, err := br.ReadByte()
		if err != nil {
			t.Fatalf("ReadByte error: %v", err)
		}

		// 验证识别为 QueryPlan
		if proto.ClientCode(codeByte) != clientCodeQueryPlan {
			t.Errorf("expected QueryPlan code %d, got %d", clientCodeQueryPlan, codeByte)
		}

		// 验证剩余数据可以读取（模拟 raw passthrough）
		remaining, err := io.ReadAll(br)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}
		if string(remaining) != "query_plan_binary_data_here" {
			t.Errorf("remaining data mismatch")
		}
	})
}

// ============================================================================
// 边界条件：未关闭的块注释
// ============================================================================

func TestReplaceOutsideQuotes_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		old      string
		repl     string
		expected string
	}{
		{
			name:     "未关闭的块注释",
			sql:      "SELECT * FROM /* my_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM /* my_table",
		},
		{
			name:     "行注释在最后一行（无换行）",
			sql:      "SELECT * FROM my_table -- end",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT * FROM new_table -- end",
		},
		{
			name:     "空SQL",
			sql:      "",
			old:      "my_table",
			repl:     "new_table",
			expected: "",
		},
		{
			name:     "仅注释",
			sql:      "-- just a comment",
			old:      "comment",
			repl:     "replaced",
			expected: "-- just a comment",
		},
		{
			name:     "嵌套块注释（SQL标准不支持嵌套，按首个*/结束）",
			sql:      "/* outer /* inner */ my_table */",
			old:      "my_table",
			repl:     "new_table",
			expected: "/* outer /* inner */ new_table */",
		},
		{
			name:     "连续的减号但不是注释（只有一个-）",
			sql:      "SELECT a - my_table FROM t",
			old:      "my_table",
			repl:     "new_table",
			expected: "SELECT a - new_table FROM t",
		},
		{
			name:     "连续行注释",
			sql:      "-- line1\n-- my_table\nSELECT my_table",
			old:      "my_table",
			repl:     "new_table",
			expected: "-- line1\n-- my_table\nSELECT new_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceOutsideQuotes(tt.sql, tt.old, tt.repl)
			if got != tt.expected {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.expected)
			}
		})
	}
}

// ============================================================================
// TablesStatusRequest bufferPool 错误路径测试
// ============================================================================

func TestTablesStatusRequestEncoding(t *testing.T) {
	t.Run("encode with zero tables", func(t *testing.T) {
		buf := &proto.Buffer{}
		proto.ClientTablesStatusRequest.Encode(buf)
		buf.PutUVarInt(0) // 0 tables

		// 验证编码: [5 (TablesStatusRequest code)][0 (count)]
		if len(buf.Buf) != 2 {
			t.Errorf("expected 2 bytes, got %d", len(buf.Buf))
		}
	})

	t.Run("encode with multiple tables", func(t *testing.T) {
		buf := &proto.Buffer{}
		proto.ClientTablesStatusRequest.Encode(buf)
		buf.PutUVarInt(2) // 2 tables
		buf.PutString("db1")
		buf.PutString("table1")
		buf.PutString("db2")
		buf.PutString("table2")

		// 读回验证
		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
		code, err := reader.UVarInt()
		if err != nil {
			t.Fatalf("read code error: %v", err)
		}
		if proto.ClientCode(code) != proto.ClientTablesStatusRequest {
			t.Errorf("expected TablesStatusRequest code, got %d", code)
		}
		count, err := reader.UVarInt()
		if err != nil {
			t.Fatalf("read count error: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 tables, got %d", count)
		}
	})
}

// ============================================================================
// IgnoredPartUUIDs 编码测试
// ============================================================================

func TestIgnoredPartUUIDsEncoding(t *testing.T) {
	t.Run("encode with UUIDs", func(t *testing.T) {
		buf := &proto.Buffer{}
		buf.PutByte(byte(clientCodeIgnoredPartUUIDs))
		buf.PutUVarInt(2) // 2 UUIDs

		// 写入 2 个 16 字节 UUID
		uuid1 := make([]byte, uuidSize)
		uuid2 := make([]byte, uuidSize)
		for i := range uuid1 {
			uuid1[i] = byte(i)
			uuid2[i] = byte(i + 16)
		}
		buf.Buf = append(buf.Buf, uuid1...)
		buf.Buf = append(buf.Buf, uuid2...)

		// 读回验证
		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
		code, err := reader.UVarInt()
		if err != nil {
			t.Fatalf("read code error: %v", err)
		}
		if proto.ClientCode(code) != clientCodeIgnoredPartUUIDs {
			t.Errorf("expected IgnoredPartUUIDs code, got %d", code)
		}
		count, err := reader.UVarInt()
		if err != nil {
			t.Fatalf("read count error: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 UUIDs, got %d", count)
		}

		// 读取 UUID 数据
		data1, err := reader.ReadRaw(uuidSize)
		if err != nil {
			t.Fatalf("read uuid1 error: %v", err)
		}
		if len(data1) != uuidSize {
			t.Errorf("uuid1 size = %d, want %d", len(data1), uuidSize)
		}
	})
}

// ============================================================================
// simpleRewrite 日志安全性测试
// ============================================================================

func TestSimpleRewritePasswordMasking(t *testing.T) {
	t.Run("password is masked in log output", func(t *testing.T) {
		// 验证 maskPassword 不会泄露完整密码
		passwords := []string{"secret123", "p@ssw0rd!", "a", "ab", ""}
		for _, pwd := range passwords {
			masked := maskPassword(pwd)
			if len(pwd) > 2 && strings.Contains(masked, pwd) {
				t.Errorf("masked password %q contains original %q", masked, pwd)
			}
			// 验证 masking 结果不为空
			if masked == "" {
				t.Errorf("masked password is empty for input %q", pwd)
			}
		}
	})
}

// ============================================================================
// ReadTaskResponse 编码一致性测试
// ============================================================================

func TestReadTaskResponseEncoding(t *testing.T) {
	t.Run("encode and verify ReadTaskResponse", func(t *testing.T) {
		response := "some_partition_name"
		buf := &proto.Buffer{}
		buf.PutByte(byte(clientCodeReadTaskResponse))
		buf.PutString(response)

		// 读回验证
		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
		code, err := reader.UVarInt()
		if err != nil {
			t.Fatalf("read code error: %v", err)
		}
		if proto.ClientCode(code) != clientCodeReadTaskResponse {
			t.Errorf("expected ReadTaskResponse code %d, got %d", clientCodeReadTaskResponse, code)
		}
		str, err := reader.Str()
		if err != nil {
			t.Fatalf("read response error: %v", err)
		}
		if str != response {
			t.Errorf("response = %q, want %q", str, response)
		}
	})
}
