package main

// query_codec.go — 自定义 ClickHouse Query 包编解码器
//
// 为什么不用 ch-go 的 Query.DecodeAware / EncodeAware？
//   1. DecodeAware 在 revision < FeatureSettingsSerializedAsStrings (54429) 时直接报错 "unsupported version"
//   2. EncodeAware 将 Stage 硬编码为 StageComplete，忽略客户端实际值
//   3. EncodeAware 遗漏 FeatureInterserverExternallyGrantedRoles 字段
//   4. ClientInfo.EncodeAware 遗漏 FeatureQueryAndLineNumbers / FeatureInterserverJWT 字段
//
// 本文件提供完全自定义的逐字段解码 & 编码，与 ClickHouse 原生 TCPHandler 精确对齐。
// 解码和编码是严格的镜像：任何条件读取的字段都会条件写回，字段顺序完全一致。

import (
	"fmt"
	log "sentioxyz/sentio-core/common/log"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/segmentio/asm/bswap"
	"go.opentelemetry.io/otel/trace"
)

// OldSetting 表示 revision < FeatureSettingsSerializedAsStrings (54429) 时的旧格式 setting。
// 旧格式: [name: String][value: UInt64]，以空名称结束。
type OldSetting struct {
	Key   string
	Value uint64
}

// ExtQuery 扩展 proto.Query，增加 ch-go struct 中未定义但协议中存在的字段。
type ExtQuery struct {
	proto.Query

	// ExternalRoles 在 FeatureInterserverExternallyGrantedRoles (>= 54472) 时存在。
	ExternalRoles string

	// 旧格式 Settings（revision < 54429 时使用）
	OldSettings []OldSetting

	// ClientInfo 中需要透传但 ch-go struct 未定义的字段
	ScriptQueryNumber uint64 // FeatureQueryAndLineNumbers (>= 54475)
	ScriptLineNumber  uint64 // FeatureQueryAndLineNumbers (>= 54475)
	HasJWT            bool   // FeatureInterserverJWT (>= 54476)
	JWT               string // FeatureInterserverJWT (>= 54476)
}

// decodeQueryCustom 按照 ClickHouse 原生协议字段顺序逐字段解码 Query 包。
// 此函数不会因为版本过低而拒绝解码——它会根据 revision 条件跳过不存在的字段。
//
// 字段顺序（与 ch-go proto/query.go DecodeAware 及 ClickHouse TCPHandler::receiveQuery 完全一致）:
//  1. QueryID           — 始终
//  2. ClientInfo         — if FeatureClientWriteInfo (>= 54420)
//  3. Settings (字符串)   — 始终按字符串格式解码（打破 ch-go 的 54429 门槛限制）
//  4. ExternalRoles      — if FeatureInterserverExternallyGrantedRoles (>= 54472)
//  5. InterServerSecret  — if FeatureInterServerSecret (>= 54441)
//  6. Stage              — 始终
//  7. Compression        — 始终
//  8. Body (SQL)          — 始终
//  9. Parameters         — if FeatureParameters (>= 54459)
func decodeQueryCustom(r *proto.Reader, revision int) (*ExtQuery, error) {
	eq := &ExtQuery{}

	// 1. QueryID
	qid, err := r.Str()
	if err != nil {
		return nil, wrapErr("QueryID", err)
	}
	eq.ID = qid

	// 2. ClientInfo
	if proto.FeatureClientWriteInfo.In(revision) {
		if err := decodeClientInfoCustom(r, &eq.Query.Info, revision, eq); err != nil {
			return nil, wrapErr("ClientInfo", err)
		}
	}

	// 3. Settings — 根据 revision 使用不同格式
	if proto.FeatureSettingsSerializedAsStrings.In(revision) {
		// 新格式: [Key: String][Flags: UVarInt][Value: String]，以空 Key 结束
		for {
			var s proto.Setting
			if err := s.Decode(r); err != nil {
				return nil, wrapErr("Setting", err)
			}
			if s.Key == "" {
				break
			}
			eq.Settings = append(eq.Settings, s)
		}
	} else {
		// 旧格式: [Key: String][Value: UInt64]，以空 Key 结束
		for {
			key, err := r.Str()
			if err != nil {
				return nil, wrapErr("OldSetting.Key", err)
			}
			if key == "" {
				break
			}
			val, err := r.Int64()
			if err != nil {
				return nil, wrapErr("OldSetting.Value", err)
			}
			eq.OldSettings = append(eq.OldSettings, OldSetting{Key: key, Value: uint64(val)})
		}
	}

	// 4. ExternalRoles
	if proto.FeatureInterserverExternallyGrantedRoles.In(revision) {
		roles, err := r.Str()
		if err != nil {
			return nil, wrapErr("ExternalRoles", err)
		}
		eq.ExternalRoles = roles
	}

	// 5. InterServerSecret
	if proto.FeatureInterServerSecret.In(revision) {
		secret, err := r.Str()
		if err != nil {
			return nil, wrapErr("InterServerSecret", err)
		}
		eq.Secret = secret
	}

	// 6. Stage
	stageVal, err := r.UVarInt()
	if err != nil {
		return nil, wrapErr("Stage", err)
	}
	eq.Stage = proto.Stage(stageVal)

	// 7. Compression
	compVal, err := r.UVarInt()
	if err != nil {
		return nil, wrapErr("Compression", err)
	}
	eq.Compression = proto.Compression(compVal)

	// 8. Body (SQL)
	body, err := r.Str()
	if err != nil {
		return nil, wrapErr("Body", err)
	}
	eq.Body = body

	// 9. Parameters
	if proto.FeatureParameters.In(revision) {
		for {
			var p proto.Parameter
			if err := p.Decode(r); err != nil {
				return nil, wrapErr("Parameter", err)
			}
			if p.Key == "" {
				break
			}
			eq.Parameters = append(eq.Parameters, p)
		}
	}

	return eq, nil
}

// encodeQueryCustom 按照与 decodeQueryCustom 严格镜像的字段顺序编码 Query 包。
// 调用者需要先写入 ClientCodeQuery 包类型字节。
func encodeQueryCustom(b *proto.Buffer, eq *ExtQuery, revision int) {
	// 包类型
	proto.ClientCodeQuery.Encode(b)

	// 1. QueryID
	b.PutString(eq.ID)

	// 2. ClientInfo
	if proto.FeatureClientWriteInfo.In(revision) {
		encodeClientInfoCustom(b, &eq.Query.Info, revision, eq)
	}

	// 3. Settings — 根据 revision 使用对应格式
	if proto.FeatureSettingsSerializedAsStrings.In(revision) {
		// 新格式
		for _, s := range eq.Settings {
			s.Encode(b)
		}
		b.PutString("") // end of settings
	} else {
		// 旧格式
		for _, s := range eq.OldSettings {
			b.PutString(s.Key)
			b.PutInt64(int64(s.Value))
		}
		b.PutString("") // end of settings
	}

	// 4. ExternalRoles
	if proto.FeatureInterserverExternallyGrantedRoles.In(revision) {
		b.PutString(eq.ExternalRoles)
	}

	// 5. InterServerSecret
	if proto.FeatureInterServerSecret.In(revision) {
		b.PutString(eq.Secret)
	}

	// 6. Stage — 使用客户端实际值，不硬编码
	eq.Stage.Encode(b)

	// 7. Compression
	eq.Compression.Encode(b)

	// 8. Body (SQL)
	b.PutString(eq.Body)

	// 9. Parameters
	if proto.FeatureParameters.In(revision) {
		for _, p := range eq.Parameters {
			p.Encode(b)
		}
		b.PutString("") // end of parameters
	}
}

// decodeClientInfoCustom 逐字段解码 ClientInfo。
// 与 ch-go proto/client_info.go DecodeAware 完全一致，并额外处理
// ch-go 解码中有但编码中遗漏的字段（QueryAndLineNumbers / InterserverJWT）。
func decodeClientInfoCustom(r *proto.Reader, info *proto.ClientInfo, revision int, eq *ExtQuery) error {
	// QueryKind
	v, err := r.UInt8()
	if err != nil {
		return wrapErr("query kind", err)
	}
	info.Query = proto.ClientQueryKind(v)

	// InitialUser
	info.InitialUser, err = r.Str()
	if err != nil {
		return wrapErr("initial user", err)
	}
	// InitialQueryID
	info.InitialQueryID, err = r.Str()
	if err != nil {
		return wrapErr("initial query id", err)
	}
	// InitialAddress
	info.InitialAddress, err = r.Str()
	if err != nil {
		return wrapErr("initial address", err)
	}

	// QueryStartTime
	if proto.FeatureQueryStartTime.In(revision) {
		info.InitialTime, err = r.Int64()
		if err != nil {
			return wrapErr("query start time", err)
		}
	}

	// Interface
	ifByte, err := r.UInt8()
	if err != nil {
		return wrapErr("interface", err)
	}
	info.Interface = proto.Interface(ifByte)

	// OSUser
	info.OSUser, err = r.Str()
	if err != nil {
		return wrapErr("os user", err)
	}
	// ClientHostname
	info.ClientHostname, err = r.Str()
	if err != nil {
		return wrapErr("client hostname", err)
	}
	// ClientName
	info.ClientName, err = r.Str()
	if err != nil {
		return wrapErr("client name", err)
	}

	// Major
	info.Major, err = r.Int()
	if err != nil {
		return wrapErr("major version", err)
	}
	// Minor
	info.Minor, err = r.Int()
	if err != nil {
		return wrapErr("minor version", err)
	}
	// ProtocolVersion
	info.ProtocolVersion, err = r.Int()
	if err != nil {
		return wrapErr("protocol version", err)
	}

	// QuotaKey
	if proto.FeatureQuotaKeyInClientInfo.In(revision) {
		info.QuotaKey, err = r.Str()
		if err != nil {
			return wrapErr("quota key", err)
		}
	}

	// DistributedDepth
	if proto.FeatureDistributedDepth.In(revision) {
		info.DistributedDepth, err = r.Int()
		if err != nil {
			return wrapErr("distributed depth", err)
		}
	}

	// Patch (only for TCP interface)
	if proto.FeatureVersionPatch.In(revision) && info.Interface == proto.InterfaceTCP {
		info.Patch, err = r.Int()
		if err != nil {
			return wrapErr("patch version", err)
		}
	}

	// OpenTelemetry
	if proto.FeatureOpenTelemetry.In(revision) {
		hasTrace, err := r.Bool()
		if err != nil {
			return wrapErr("open telemetry flag", err)
		}
		if hasTrace {
			var cfg trace.SpanContextConfig
			// TraceID (16 bytes, need bswap)
			traceIDBytes, err := r.ReadRaw(16)
			if err != nil {
				return wrapErr("trace id", err)
			}
			bswap.Swap64(traceIDBytes)
			copy(cfg.TraceID[:], traceIDBytes)

			// SpanID (8 bytes, need bswap)
			spanIDBytes, err := r.ReadRaw(8)
			if err != nil {
				return wrapErr("span id", err)
			}
			bswap.Swap64(spanIDBytes)
			copy(cfg.SpanID[:], spanIDBytes)

			// TraceState
			tsStr, err := r.Str()
			if err != nil {
				return wrapErr("trace state", err)
			}
			if tsStr != "" {
				ts, err := trace.ParseTraceState(tsStr)
				if err != nil {
					return wrapErr("parse trace state", err)
				}
				cfg.TraceState = ts
			}

			// TraceFlags
			tf, err := r.Byte()
			if err != nil {
				return wrapErr("trace flags", err)
			}
			cfg.TraceFlags = trace.TraceFlags(tf)

			info.Span = trace.NewSpanContext(cfg)
		}
	}

	// ParallelReplicas
	if proto.FeatureParallelReplicas.In(revision) {
		collaborateVal, err := r.Int()
		if err != nil {
			return wrapErr("parallel replicas", err)
		}
		info.CollaborateWithInitiator = collaborateVal == 1

		info.CountParticipatingReplicas, err = r.Int()
		if err != nil {
			return wrapErr("count participating replicas", err)
		}
		info.NumberOfCurrentReplica, err = r.Int()
		if err != nil {
			return wrapErr("number of current replica", err)
		}
	}

	// QueryAndLineNumbers
	if proto.FeatureQueryAndLineNumbers.In(revision) {
		eq.ScriptQueryNumber, err = r.UVarInt()
		if err != nil {
			return wrapErr("script query number", err)
		}
		eq.ScriptLineNumber, err = r.UVarInt()
		if err != nil {
			return wrapErr("script line number", err)
		}
	}

	// InterserverJWT
	if proto.FeatureInterserverJWT.In(revision) {
		eq.HasJWT, err = r.Bool()
		if err != nil {
			return wrapErr("jwt flag", err)
		}
		if eq.HasJWT {
			eq.JWT, err = r.Str()
			if err != nil {
				return wrapErr("jwt", err)
			}
		}
	}

	return nil
}

// encodeClientInfoCustom 按照与 decodeClientInfoCustom 严格镜像的字段顺序编码 ClientInfo。
// 补充了 ch-go EncodeAware 中遗漏的 FeatureQueryAndLineNumbers 和 FeatureInterserverJWT 字段。
func encodeClientInfoCustom(b *proto.Buffer, info *proto.ClientInfo, revision int, eq *ExtQuery) {
	// QueryKind
	b.PutByte(byte(info.Query))

	// InitialUser / InitialQueryID / InitialAddress
	b.PutString(info.InitialUser)
	b.PutString(info.InitialQueryID)
	b.PutString(info.InitialAddress)

	// QueryStartTime
	if proto.FeatureQueryStartTime.In(revision) {
		b.PutInt64(info.InitialTime)
	}

	// Interface
	b.PutByte(byte(info.Interface))

	// OSUser / ClientHostname / ClientName
	b.PutString(info.OSUser)
	b.PutString(info.ClientHostname)
	b.PutString(info.ClientName)

	// Major / Minor / ProtocolVersion
	b.PutInt(info.Major)
	b.PutInt(info.Minor)
	b.PutInt(info.ProtocolVersion)

	// QuotaKey
	if proto.FeatureQuotaKeyInClientInfo.In(revision) {
		b.PutString(info.QuotaKey)
	}

	// DistributedDepth
	if proto.FeatureDistributedDepth.In(revision) {
		b.PutInt(info.DistributedDepth)
	}

	// Patch (only for TCP interface)
	if proto.FeatureVersionPatch.In(revision) && info.Interface == proto.InterfaceTCP {
		b.PutInt(info.Patch)
	}

	// OpenTelemetry
	if proto.FeatureOpenTelemetry.In(revision) {
		if info.Span.IsValid() {
			b.PutByte(1)
			{
				v := info.Span.TraceID()
				start := len(b.Buf)
				b.Buf = append(b.Buf, v[:]...)
				bswap.Swap64(b.Buf[start:])
			}
			{
				v := info.Span.SpanID()
				start := len(b.Buf)
				b.Buf = append(b.Buf, v[:]...)
				bswap.Swap64(b.Buf[start:])
			}
			b.PutString(info.Span.TraceState().String())
			b.PutByte(byte(info.Span.TraceFlags()))
		} else {
			b.PutByte(0)
		}
	}

	// ParallelReplicas
	if proto.FeatureParallelReplicas.In(revision) {
		if info.CollaborateWithInitiator {
			b.PutInt(1)
		} else {
			b.PutInt(0)
		}
		b.PutInt(info.CountParticipatingReplicas)
		b.PutInt(info.NumberOfCurrentReplica)
	}

	// QueryAndLineNumbers — ch-go EncodeAware 遗漏的字段
	if proto.FeatureQueryAndLineNumbers.In(revision) {
		b.PutUVarInt(eq.ScriptQueryNumber)
		b.PutUVarInt(eq.ScriptLineNumber)
	}

	// InterserverJWT — ch-go EncodeAware 遗漏的字段
	if proto.FeatureInterserverJWT.In(revision) {
		if eq.HasJWT {
			b.PutByte(1)
			b.PutString(eq.JWT)
		} else {
			b.PutByte(0)
		}
	}
}

// wrapErr 是简单的错误包装辅助函数
func wrapErr(field string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("decode %s: %w", field, err)
}

// resultsToInput 将 Results（DecodeBlock 的产出）转换为 []InputColumn（EncodeBlock 的输入）。
// 这是 Data 块透传的关键桥梁：解码得到 Results，编码需要 InputColumn。
// ColAuto.Data 实现了 Column 接口（同时包含 ColResult 和 ColInput），所以可以直接用于编码。
func resultsToInput(results proto.Results) ([]proto.InputColumn, error) {
	cols := make([]proto.InputColumn, len(results))
	for i, rc := range results {
		colInput, ok := rc.Data.(proto.ColInput)
		if !ok {
			return nil, fmt.Errorf("column %q (type %T) does not implement ColInput", rc.Name, rc.Data)
		}
		cols[i] = proto.InputColumn{
			Name: rc.Name,
			Data: colInput,
		}
	}
	return cols, nil
}

// ========== 兼容 BlockInfo 解码/编码 ==========
// ClickHouse 26.x 在 BlockInfo 中新增了 field 3 (out_of_order_buckets, vector<Int32>)。
// ch-go v1.0.1 的 BlockInfo.Decode 遇到未知 field 直接报错。
// 以下自定义实现支持 field 1, 2, 3 并正确透传。

// BlockInfoCompat 扩展 ch-go 的 BlockInfo，支持 field 3。
type BlockInfoCompat struct {
	Overflows         bool
	BucketNum         int
	OutOfOrderBuckets []int32 // field 3: ClickHouse 26.x 新增
}

func decodeBlockInfoCompat(r *proto.Reader) (*BlockInfoCompat, error) {
	info := &BlockInfoCompat{BucketNum: -1}
	for {
		f, err := r.UVarInt()
		if err != nil {
			return nil, fmt.Errorf("field id: %w", err)
		}
		switch f {
		case 0: // end
			return info, nil
		case 1: // is_overflows (bool)
			v, err := r.Bool()
			if err != nil {
				return nil, fmt.Errorf("overflows: %w", err)
			}
			info.Overflows = v
		case 2: // bucket_num (Int32)
			v, err := r.Int32()
			if err != nil {
				return nil, fmt.Errorf("bucket_num: %w", err)
			}
			info.BucketNum = int(v)
		case 3: // out_of_order_buckets (vector<Int32>)
			// readBinary(vector<Int32>) = [count: UInt64][Int32 × count]
			count, err := r.UVarInt()
			if err != nil {
				return nil, fmt.Errorf("out_of_order_buckets count: %w", err)
			}
			buckets := make([]int32, count)
			for i := uint64(0); i < count; i++ {
				v, err := r.Int32()
				if err != nil {
					return nil, fmt.Errorf("out_of_order_buckets[%d]: %w", i, err)
				}
				buckets[i] = v
			}
			info.OutOfOrderBuckets = buckets
		default:
			// 容错处理：未知 field ID 尝试跳过一个 UVarInt 值
			// 当 ClickHouse 新版本添加新 field 时不会导致连接断开
			log.Warnf("decodeBlockInfoCompat: unknown field %d, attempting to skip", f)
			if _, err := r.UVarInt(); err != nil {
				return nil, fmt.Errorf("unknown BlockInfo field %d: skip failed: %w", f, err)
			}
		}
	}
}

func encodeBlockInfoCompat(b *proto.Buffer, info *BlockInfoCompat) {
	// field 1: is_overflows
	b.PutUVarInt(1)
	b.PutBool(info.Overflows)
	// field 2: bucket_num
	b.PutUVarInt(2)
	b.PutInt32(int32(info.BucketNum))
	// field 3: out_of_order_buckets (only if non-empty)
	if len(info.OutOfOrderBuckets) > 0 {
		b.PutUVarInt(3)
		b.PutUVarInt(uint64(len(info.OutOfOrderBuckets)))
		for _, v := range info.OutOfOrderBuckets {
			b.PutInt32(v)
		}
	}
	// end
	b.PutUVarInt(0)
}
