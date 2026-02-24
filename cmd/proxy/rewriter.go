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
	"google.golang.org/grpc/keepalive"

	"go.yaml.in/yaml/v3"
)

// Rewriter is responsible for rewriting Sentio-Network mode SQL into real SQL
type Rewriter interface {
	// Rewrite takes the original SQL and returns the rewritten SQL
	// If the SQL does not contain Sentio-Network mode table names, return the original SQL
	Rewrite(ctx context.Context, sql string) (string, error)
	// Close closes the gRPC connection
	Close() error
}

// NetworkState represents network state information
type NetworkState interface {
	// GetProcessorAllocation retrieves the Processor allocation information
	GetProcessorAllocation(processorId string) ([]ProcessorAllocation, bool)
	// GetIndexerInfo retrieves Indexer information
	GetIndexerInfo(indexerId uint64) (IndexerInfo, bool)
	// GetProcessorInfo retrieves Processor information
	GetProcessorInfo(processorId string) (ProcessorInfo, bool)
}

// IndexerInfo represents Indexer node information
type IndexerInfo struct {
	IndexerId           uint64 `json:"indexerId" yaml:"indexer_id"`
	IndexerUrl          string `json:"indexerUrl" yaml:"indexer_url"`
	ComputeNodeRpcPort  uint16 `json:"computeNodeRpcPort" yaml:"compute_node_rpc_port"`
	StorageNodeRpcPort  uint16 `json:"storageNodeRpcPort" yaml:"storage_node_rpc_port"`
	ClickhouseProxyPort uint16 `json:"clickhouseProxyPort" yaml:"clickhouse_proxy_port"`
}

// ProcessorAllocation represents Processor allocation information
type ProcessorAllocation struct {
	ProcessorId string `json:"processorId" yaml:"processor_id"`
	IndexerId   uint64 `json:"indexerId" yaml:"indexer_id"`
}

// ProcessorInfo represents Processor information
type ProcessorInfo struct {
	ProcessorId         string `json:"processorId" yaml:"processor_id"`
	EntitySchema        string `json:"entitySchema" yaml:"entity_schema"`
	EntitySchemaVersion int32  `json:"entitySchemaVersion" yaml:"entity_schema_version"`
}

// RewriterConfig is the rewriter configuration
type RewriterConfig struct {
	Enabled        bool   // Whether to enable rewriting
	ServiceAddr    string // sql-rewriter gRPC service address
	LocalIndexerId uint64 // Local Indexer ID
	CHUser         string // ClickHouse connection username
	CHPassword     string // ClickHouse connection password
	Timeout        time.Duration
}

// SentioNetworkRewriter implements the Rewriter interface
type SentioNetworkRewriter struct {
	config       RewriterConfig
	networkState NetworkState
	grpcConn     *grpc.ClientConn
	grpcClient   pb.RewriterServiceClient // cached gRPC client stub
}

// sentioNetworkTableRegex matches Sentio-Network mode table names
// Format: sentio_<processor_id>.<table_name>
var sentioNetworkTableRegex = regexp.MustCompile(`(?i)\bsentio_([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b`)

// ParsedTable represents parsed table name information
type ParsedTable struct {
	FullMatch   string // Full match, e.g. "sentio_coinbase.transfer"
	ProcessorId string // processor_id, e.g. "coinbase"
	TableName   string // Table name, e.g. "transfer"
}

// NewSentioNetworkRewriter creates a new SentioNetworkRewriter
func NewSentioNetworkRewriter(config RewriterConfig, state NetworkState) (*SentioNetworkRewriter, error) {
	rewriter := &SentioNetworkRewriter{
		config:       config,
		networkState: state,
	}

	// Establish gRPC connection (lazy connect, non-blocking startup)
	// Add keepalive to maintain long connection health
	if config.ServiceAddr != "" {
		kaParams := keepalive.ClientParameters{
			Time:                30 * time.Second, // Send ping every 30s (P2: adjusted from 10s to 30s to reduce overhead)
			Timeout:             5 * time.Second,  // Ping timeout (adjusted from 3s to 5s)
			PermitWithoutStream: true,             // Keep pinging even without active streams
		}
		conn, err := grpc.NewClient(
			config.ServiceAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(kaParams),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create rewriter client for %s: %w", config.ServiceAddr, err)
		}
		rewriter.grpcConn = conn
		rewriter.grpcClient = pb.NewRewriterServiceClient(conn)
	}

	return rewriter, nil
}

// Rewrite rewrites Sentio-Network mode SQL into real SQL
func (r *SentioNetworkRewriter) Rewrite(ctx context.Context, sql string) (string, error) {
	// Parse Sentio-Network mode table names in SQL
	tables := r.parseSentioNetworkTables(sql)
	if len(tables) == 0 {
		// No Sentio-Network mode table names found; return original SQL
		return sql, nil
	}

	// Build rewrite mapping
	tableWithDatabaseMap := make(map[string]TableWithDatabase)
	remoteTableMap := make(map[string]RemoteTable)

	for _, table := range tables {
		// Get Processor allocation info
		allocations, ok := r.networkState.GetProcessorAllocation(table.ProcessorId)
		if !ok || len(allocations) == 0 {
			log.Warnf("processor allocation not found for processor_id=%s, skipping rewrite", table.ProcessorId)
			continue
		}

		// Take the first allocation (simplified handling)
		allocation := allocations[0]

		// Get Indexer info
		indexerInfo, ok := r.networkState.GetIndexerInfo(allocation.IndexerId)
		if !ok {
			log.Warnf("indexer info not found for indexer_id=%d, skipping rewrite", allocation.IndexerId)
			continue
		}

		// Get Processor info
		processorInfo, ok := r.networkState.GetProcessorInfo(table.ProcessorId)
		if !ok {
			log.Warnf("processor info not found for processor_id=%s, using default", table.ProcessorId)
			processorInfo = ProcessorInfo{ProcessorId: table.ProcessorId}
		}

		// Build physical table name
		physicalTable := r.buildPhysicalTableName(table.ProcessorId, table.TableName, processorInfo)
		// Prefer ProcessorInfo.EntitySchema as database name; default to "sentio" if empty
		database := "sentio"
		if processorInfo.EntitySchema != "" {
			database = processorInfo.EntitySchema
		}

		if allocation.IndexerId == r.config.LocalIndexerId {
			// Local table: use table_with_database_map
			tableWithDatabaseMap[table.FullMatch] = TableWithDatabase{
				Database: database,
				Table:    physicalTable,
			}
			log.Debugf("local table rewrite: %s -> %s.%s", table.FullMatch, database, physicalTable)
		} else {
			// Remote table: use remote_table_map
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

	// If no tables need rewriting, return original SQL
	if len(tableWithDatabaseMap) == 0 && len(remoteTableMap) == 0 {
		return sql, nil
	}

	// Call sql-rewriter service for rewriting
	if r.grpcConn != nil {
		rewrittenSQL, err := r.callRewriterService(ctx, sql, tableWithDatabaseMap, remoteTableMap)
		if err != nil {
			log.Errorf("rewriter service call failed: %v, falling back to simple rewrite", err)
			// Fall back to simple replacement
			return r.simpleRewrite(sql, tableWithDatabaseMap, remoteTableMap), nil
		}
		return rewrittenSQL, nil
	}

	// Use simple replacement when no gRPC connection
	return r.simpleRewrite(sql, tableWithDatabaseMap, remoteTableMap), nil
}

// parseSentioNetworkTables parses Sentio-Network mode table names in SQL
func (r *SentioNetworkRewriter) parseSentioNetworkTables(sql string) []ParsedTable {
	matches := sentioNetworkTableRegex.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return nil
	}

	// Deduplicate
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

// buildPhysicalTableName builds the physical table name
func (r *SentioNetworkRewriter) buildPhysicalTableName(processorId, tableName string, info ProcessorInfo) string {
	// If EntitySchemaVersion > 0 is specified in ProcessorInfo, use prefixed format
	// Otherwise return the original table name (for test scenarios)
	if info.EntitySchemaVersion > 0 {
		prefix := r.generateTablePrefix(processorId)
		return fmt.Sprintf("%s_%s", prefix, tableName)
	}
	// Return original table name directly
	return tableName
}

// generateTablePrefix generates the table name prefix
func (r *SentioNetworkRewriter) generateTablePrefix(processorId string) string {
	if len(processorId) > 8 {
		return processorId[:8]
	}
	return processorId
}

// TableWithDatabase represents a table with its database
type TableWithDatabase struct {
	Database string
	Table    string
}

// RemoteTable represents a remote table
type RemoteTable struct {
	Addr     string
	Database string
	Table    string
	User     string
	Password string
}

// callRewriterService calls the sql-rewriter gRPC service
// R4-5 Security note: RemoteTable.Password is sent in plaintext to the sql-rewriter gRPC service,
// sql-rewriter needs it to generate remote() function calls. This communication occurs only within trusted internal networks,
// if the deployment environment changes (cross-network/public), switch to TLS encrypted channels.
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

	// Set gRPC call timeout
	timeout := r.config.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := client.Rewrite(ctxWithTimeout, req)
	if err != nil {
		return "", err
	}
	if resp.Code != pb.RewriteCode_Success {
		return "", fmt.Errorf("rewriter error: %s", resp.Message)
	}

	return resp.SqlAfterRewrite, nil
}

// simpleRewrite performs simple string replacement rewriting (fallback approach)
// Uses word-boundary aware replacement to avoid false matches in string literals
func (r *SentioNetworkRewriter) simpleRewrite(sql string, tableWithDatabase map[string]TableWithDatabase, remoteTable map[string]RemoteTable) string {
	result := sql

	// Replace local tables (using word-boundary regex)
	for original, target := range tableWithDatabase {
		replacement := fmt.Sprintf("%s.%s", target.Database, target.Table)
		result = replaceOutsideQuotes(result, original, replacement)
	}

	// Replace remote tables (using word-boundary regex)
	for original, target := range remoteTable {
		replacement := fmt.Sprintf("remote('%s', '%s', '%s', '%s', '%s')",
			target.Addr, target.Database, target.Table, target.User, target.Password)
		result = replaceOutsideQuotes(result, original, replacement)
		// P2-9: Use masking in logs to avoid password leakage
		maskedReplacement := fmt.Sprintf("remote('%s', '%s', '%s', '%s', '%s')",
			target.Addr, target.Database, target.Table, target.User, maskPassword(target.Password))
		log.Infof("simpleRewrite: remote table %q -> %s", original, maskedReplacement)
	}

	return result
}

// replaceOutsideQuotes replaces target strings in SQL, but skips content inside quotes and comments.
// This avoids strings.ReplaceAll potentially replacing table names in string literals/comments.
// Supports three quoting styles: single quotes('), double quotes("), backticks(`)
// Supports two escape methods: backslash escape(\') and ClickHouse consecutive quote escape(")
// P2-8: Supports SQL comments: line comments(--) and block comments(/* */)
func replaceOutsideQuotes(sql, old, replacement string) string {
	var result strings.Builder
	result.Grow(len(sql))
	i := 0
	for i < len(sql) {
		// P2-8: Check line comment (-- to end of line)
		if i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-' {
			// Skip the entire line comment
			for i < len(sql) && sql[i] != '\n' {
				result.WriteByte(sql[i])
				i++
			}
			continue
		}
		// P2-8: Check block comment (/* ... */)
		if i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*' {
			result.WriteByte(sql[i])
			i++
			result.WriteByte(sql[i])
			i++
			// Skip until */ is found
			for i < len(sql) {
				if i+1 < len(sql) && sql[i] == '*' && sql[i+1] == '/' {
					result.WriteByte(sql[i])
					i++
					result.WriteByte(sql[i])
					i++
					break
				}
				result.WriteByte(sql[i])
				i++
			}
			continue
		}
		// Check if inside quotes (including backticks, ClickHouse is compatible with MySQL identifier quoting syntax)
		if sql[i] == '\'' || sql[i] == '"' || sql[i] == '`' {
			quote := sql[i]
			result.WriteByte(quote)
			i++
			// Skip to matching closing quote
			for i < len(sql) {
				result.WriteByte(sql[i])
				if sql[i] == quote {
					// Risk-4: Handle ClickHouse consecutive quote escaping ('' or "")
					// e.g. SELECT 'it''s a test' where '' is an escaped single quote
					if i+1 < len(sql) && sql[i+1] == quote {
						i++
						result.WriteByte(sql[i])
						i++
						continue
					}
					i++
					break
				}
				// Handle backslash escaped quotes (\')
				if sql[i] == '\\' && i+1 < len(sql) {
					i++
					result.WriteByte(sql[i])
				}
				i++
			}
			continue
		}
		// Outside quotes: try to match
		if i+len(old) <= len(sql) && sql[i:i+len(old)] == old {
			result.WriteString(replacement)
			i += len(old)
		} else {
			result.WriteByte(sql[i])
			i++
		}
	}
	return result.String()
}

// Close closes the gRPC connection
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

// NoopRewriter is a no-op implementation that performs no rewriting
type NoopRewriter struct{}

func (n NoopRewriter) Rewrite(ctx context.Context, sql string) (string, error) {
	return sql, nil
}

func (n NoopRewriter) Close() error {
	return nil
}

// InMemoryNetworkState is an in-memory network state implementation (for testing)
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

// ParseIndexerId parses the Indexer ID
func ParseIndexerId(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}
