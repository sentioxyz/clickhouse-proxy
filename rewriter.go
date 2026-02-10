package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	log "sentioxyz/sentio-core/common/log"

	pb "ck_remote_proxy/protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.yaml.in/yaml/v3"
)

// Rewriter 负责将 Sentio-Network 模式 SQL 重写为真实 SQL
type Rewriter interface {
	// Rewrite 接收原始 SQL，返回重写后的 SQL
	// 如果 SQL 不包含 Sentio-Network 模式表名，返回原 SQL
	Rewrite(ctx context.Context, sql string) (string, error)
	// Close 关闭 gRPC 连接
	Close() error
}

// NetworkState 表示网络状态信息
type NetworkState interface {
	// GetProcessorAllocation 获取 Processor 的分配信息
	GetProcessorAllocation(processorId string) ([]ProcessorAllocation, bool)
	// GetIndexerInfo 获取 Indexer 信息
	GetIndexerInfo(indexerId uint64) (IndexerInfo, bool)
	// GetProcessorInfo 获取 Processor 信息
	GetProcessorInfo(processorId string) (ProcessorInfo, bool)
}

// IndexerInfo 表示 Indexer 节点信息
type IndexerInfo struct {
	IndexerId           uint64 `json:"indexerId" yaml:"indexer_id"`
	IndexerUrl          string `json:"indexerUrl" yaml:"indexer_url"`
	ComputeNodeRpcPort  uint16 `json:"computeNodeRpcPort" yaml:"compute_node_rpc_port"`
	StorageNodeRpcPort  uint16 `json:"storageNodeRpcPort" yaml:"storage_node_rpc_port"`
	ClickhouseProxyPort uint16 `json:"clickhouseProxyPort" yaml:"clickhouse_proxy_port"`
}

// ProcessorAllocation 表示 Processor 分配信息
type ProcessorAllocation struct {
	ProcessorId string `json:"processorId" yaml:"processor_id"`
	IndexerId   uint64 `json:"indexerId" yaml:"indexer_id"`
}

// ProcessorInfo 表示 Processor 信息
type ProcessorInfo struct {
	ProcessorId         string `json:"processorId" yaml:"processor_id"`
	EntitySchema        string `json:"entitySchema" yaml:"entity_schema"`
	EntitySchemaVersion int32  `json:"entitySchemaVersion" yaml:"entity_schema_version"`
}

// RewriterConfig 重写器配置
type RewriterConfig struct {
	Enabled        bool   // 是否启用重写
	ServiceAddr    string // sql-rewriter gRPC 服务地址
	LocalIndexerId uint64 // 本地 Indexer ID
	CHUser         string // ClickHouse 连接用户名
	CHPassword     string // ClickHouse 连接密码
	Timeout        time.Duration
}

// SentioNetworkRewriter 实现 Rewriter 接口
type SentioNetworkRewriter struct {
	config       RewriterConfig
	networkState NetworkState
	grpcConn     *grpc.ClientConn
	grpcClient   pb.RewriterServiceClient // cached gRPC client stub
}

// sentioNetworkTableRegex 匹配 Sentio-Network 模式表名
// 格式: sentio_<processor_id>.<table_name>
var sentioNetworkTableRegex = regexp.MustCompile(`(?i)\bsentio_([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b`)

// ParsedTable 表示解析后的表名信息
type ParsedTable struct {
	FullMatch   string // 完整匹配，如 "sentio_coinbase.transfer"
	ProcessorId string // processor_id，如 "coinbase"
	TableName   string // 表名，如 "transfer"
}

// NewSentioNetworkRewriter 创建一个新的 SentioNetworkRewriter
func NewSentioNetworkRewriter(config RewriterConfig, state NetworkState) (*SentioNetworkRewriter, error) {
	rewriter := &SentioNetworkRewriter{
		config:       config,
		networkState: state,
	}

	// 建立 gRPC 连接 (lazy connect, 不阻塞启动)
	if config.ServiceAddr != "" {
		conn, err := grpc.NewClient(
			config.ServiceAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create rewriter client for %s: %w", config.ServiceAddr, err)
		}
		rewriter.grpcConn = conn
		rewriter.grpcClient = pb.NewRewriterServiceClient(conn)
	}

	return rewriter, nil
}

// Rewrite 将 Sentio-Network 模式 SQL 重写为真实 SQL
func (r *SentioNetworkRewriter) Rewrite(ctx context.Context, sql string) (string, error) {
	// 解析 SQL 中的 Sentio-Network 模式表名
	tables := r.parseSentioNetworkTables(sql)
	if len(tables) == 0 {
		// 没有 Sentio-Network 模式表名，返回原 SQL
		return sql, nil
	}

	// 构建重写映射
	tableWithDatabaseMap := make(map[string]TableWithDatabase)
	remoteTableMap := make(map[string]RemoteTable)

	for _, table := range tables {
		// 获取 Processor 分配信息
		allocations, ok := r.networkState.GetProcessorAllocation(table.ProcessorId)
		if !ok || len(allocations) == 0 {
			log.Warnf("processor allocation not found for processor_id=%s, skipping rewrite", table.ProcessorId)
			continue
		}

		// 取第一个分配（简化处理）
		allocation := allocations[0]

		// 获取 Indexer 信息
		indexerInfo, ok := r.networkState.GetIndexerInfo(allocation.IndexerId)
		if !ok {
			log.Warnf("indexer info not found for indexer_id=%d, skipping rewrite", allocation.IndexerId)
			continue
		}

		// 获取 Processor 信息
		processorInfo, ok := r.networkState.GetProcessorInfo(table.ProcessorId)
		if !ok {
			log.Warnf("processor info not found for processor_id=%s, using default", table.ProcessorId)
			processorInfo = ProcessorInfo{ProcessorId: table.ProcessorId}
		}

		// 构建物理表名
		physicalTable := r.buildPhysicalTableName(table.ProcessorId, table.TableName, processorInfo)
		// 优先使用 ProcessorInfo.EntitySchema 作为数据库名，若为空则默认 "sentio"
		database := "sentio"
		if processorInfo.EntitySchema != "" {
			database = processorInfo.EntitySchema
		}

		if allocation.IndexerId == r.config.LocalIndexerId {
			// 本地表：使用 table_with_database_map
			tableWithDatabaseMap[table.FullMatch] = TableWithDatabase{
				Database: database,
				Table:    physicalTable,
			}
			log.Debugf("local table rewrite: %s -> %s.%s", table.FullMatch, database, physicalTable)
		} else {
			// 远程表：使用 remote_table_map
			addr := fmt.Sprintf("%s:%d", indexerInfo.IndexerUrl, indexerInfo.ClickhouseProxyPort)
			remoteTableMap[table.FullMatch] = RemoteTable{
				Addr:     addr,
				Database: database,
				Table:    physicalTable,
				User:     r.config.CHUser,
				Password: r.config.CHPassword,
			}
			log.Debugf("remote table rewrite: %s -> remote('%s', '%s', '%s', '%s', '***')",
				table.FullMatch, addr, database, physicalTable, r.config.CHUser)
		}
	}

	// 如果没有需要重写的表，返回原 SQL
	if len(tableWithDatabaseMap) == 0 && len(remoteTableMap) == 0 {
		return sql, nil
	}

	// 调用 sql-rewriter 服务进行重写
	if r.grpcConn != nil {
		rewrittenSQL, err := r.callRewriterService(ctx, sql, tableWithDatabaseMap, remoteTableMap)
		if err != nil {
			log.Errorf("rewriter service call failed: %v, falling back to simple rewrite", err)
			// 降级到简单替换
			return r.simpleRewrite(sql, tableWithDatabaseMap, remoteTableMap), nil
		}
		return rewrittenSQL, nil
	}

	// 无 gRPC 连接时使用简单替换
	return r.simpleRewrite(sql, tableWithDatabaseMap, remoteTableMap), nil
}

// parseSentioNetworkTables 解析 SQL 中的 Sentio-Network 模式表名
func (r *SentioNetworkRewriter) parseSentioNetworkTables(sql string) []ParsedTable {
	matches := sentioNetworkTableRegex.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return nil
	}

	// 去重
	seen := make(map[string]bool)
	var tables []ParsedTable
	for _, match := range matches {
		if len(match) != 3 {
			continue
		}
		fullMatch := match[0]
		if seen[fullMatch] {
			continue
		}
		seen[fullMatch] = true
		tables = append(tables, ParsedTable{
			FullMatch:   fullMatch,
			ProcessorId: match[1],
			TableName:   match[2],
		})
	}
	return tables
}

// buildPhysicalTableName 构建物理表名
func (r *SentioNetworkRewriter) buildPhysicalTableName(processorId, tableName string, info ProcessorInfo) string {
	// 如果 ProcessorInfo 中指定了 EntitySchemaVersion > 0，使用带前缀格式
	// 否则直接返回原始表名（用于测试场景）
	if info.EntitySchemaVersion > 0 {
		prefix := r.generateTablePrefix(processorId)
		return fmt.Sprintf("%s_%s", prefix, tableName)
	}
	// 直接返回原始表名
	return tableName
}

// generateTablePrefix 生成表名前缀
func (r *SentioNetworkRewriter) generateTablePrefix(processorId string) string {
	if len(processorId) > 8 {
		return processorId[:8]
	}
	return processorId
}

// TableWithDatabase 表示带数据库的表
type TableWithDatabase struct {
	Database string
	Table    string
}

// RemoteTable 表示远程表
type RemoteTable struct {
	Addr     string
	Database string
	Table    string
	User     string
	Password string
}

// callRewriterService 调用 sql-rewriter gRPC 服务
func (r *SentioNetworkRewriter) callRewriterService(ctx context.Context, sql string, tableWithDatabase map[string]TableWithDatabase, remoteTable map[string]RemoteTable) (string, error) {
	client := r.grpcClient

	// Convert maps to proto
	tableNameArgs := &pb.RewriteTableNameArgs{
		TableWithDatabaseMap: make(map[string]*pb.RewriteTableNameArgs_TableWithDatabase),
		RemoteTableMap:       make(map[string]*pb.RewriteTableNameArgs_RemoteTable),
	}

	for k, v := range tableWithDatabase {
		tableNameArgs.TableWithDatabaseMap[k] = &pb.RewriteTableNameArgs_TableWithDatabase{
			Database: v.Database,
			Table:    v.Table,
		}
	}

	for k, v := range remoteTable {
		tableNameArgs.RemoteTableMap[k] = &pb.RewriteTableNameArgs_RemoteTable{
			Addr:     v.Addr,
			Database: v.Database,
			Table:    v.Table,
			User:     v.User,
			Password: v.Password,
		}
	}

	req := &pb.RewriteSQLRequest{
		Sql: sql,
		Options: []*pb.RewriteOption{
			{
				Op: pb.RewriteOp_TableNameRewrite,
				Value: &pb.RewriteOption_TableNameArgs{
					TableNameArgs: tableNameArgs,
				},
			},
		},
	}

	resp, err := client.Rewrite(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.Code != pb.RewriteCode_Success {
		return "", fmt.Errorf("rewriter error: %s", resp.Message)
	}

	return resp.SqlAfterRewrite, nil
}

// simpleRewrite 简单字符串替换重写（降级方案）
func (r *SentioNetworkRewriter) simpleRewrite(sql string, tableWithDatabase map[string]TableWithDatabase, remoteTable map[string]RemoteTable) string {
	result := sql

	// 替换本地表
	for original, target := range tableWithDatabase {
		replacement := fmt.Sprintf("%s.%s", target.Database, target.Table)
		result = strings.ReplaceAll(result, original, replacement)
	}

	// 替换远程表
	for original, target := range remoteTable {
		replacement := fmt.Sprintf("remote('%s', '%s', '%s', '%s', '%s')",
			target.Addr, target.Database, target.Table, target.User, target.Password)
		result = strings.ReplaceAll(result, original, replacement)
	}

	return result
}

// Close 关闭 gRPC 连接
func (r *SentioNetworkRewriter) Close() error {
	if r.grpcConn != nil {
		return r.grpcConn.Close()
	}
	return nil
}

// maskPassword masks a password string for safe logging.
func maskPassword(password string) string {
	if len(password) <= 2 {
		return "***"
	}
	return password[:1] + strings.Repeat("*", len(password)-2) + password[len(password)-1:]
}

// NoopRewriter 空实现，不进行任何重写
type NoopRewriter struct{}

func (n NoopRewriter) Rewrite(ctx context.Context, sql string) (string, error) {
	return sql, nil
}

func (n NoopRewriter) Close() error {
	return nil
}

// InMemoryNetworkState 内存中的网络状态实现（用于测试）
type InMemoryNetworkState struct {
	mu                   sync.RWMutex
	ProcessorAllocations map[string][]ProcessorAllocation
	IndexerInfos         map[uint64]IndexerInfo
	ProcessorInfos       map[string]ProcessorInfo
}

func NewInMemoryNetworkState() *InMemoryNetworkState {
	return &InMemoryNetworkState{
		ProcessorAllocations: make(map[string][]ProcessorAllocation),
		IndexerInfos:         make(map[uint64]IndexerInfo),
		ProcessorInfos:       make(map[string]ProcessorInfo),
	}
}

func (s *InMemoryNetworkState) GetProcessorAllocation(processorId string) ([]ProcessorAllocation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocs, ok := s.ProcessorAllocations[processorId]
	return allocs, ok
}

func (s *InMemoryNetworkState) GetIndexerInfo(indexerId uint64) (IndexerInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.IndexerInfos[indexerId]
	return info, ok
}

func (s *InMemoryNetworkState) GetProcessorInfo(processorId string) (ProcessorInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.ProcessorInfos[processorId]
	return info, ok
}

// networkStateYAML defines the YAML file structure for network state.
type networkStateYAML struct {
	IndexerInfos         map[uint64]IndexerInfo           `yaml:"indexer_infos"`
	ProcessorAllocations map[string][]ProcessorAllocation `yaml:"processor_allocations"`
	ProcessorInfos       map[string]ProcessorInfo         `yaml:"processor_infos"`
}

// LoadNetworkStateFromYAML loads network state from a YAML configuration file.
func LoadNetworkStateFromYAML(path string) (*InMemoryNetworkState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read network state file %s: %w", path, err)
	}

	var raw networkStateYAML
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse network state YAML %s: %w", path, err)
	}

	state := NewInMemoryNetworkState()
	for id, info := range raw.IndexerInfos {
		state.IndexerInfos[id] = info
	}
	for pid, allocs := range raw.ProcessorAllocations {
		state.ProcessorAllocations[pid] = allocs
	}
	for pid, info := range raw.ProcessorInfos {
		state.ProcessorInfos[pid] = info
	}

	log.Infof("loaded network state from %s: %d indexers, %d processor allocations, %d processor infos",
		path, len(state.IndexerInfos), len(state.ProcessorAllocations), len(state.ProcessorInfos))

	return state, nil
}

// --- Helper functions for parsing ---

// ParseIndexerId 解析 Indexer ID
func ParseIndexerId(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}
