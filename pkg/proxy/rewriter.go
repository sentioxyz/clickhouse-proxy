package proxy

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
	"sentioxyz/sentio-core/network/state"

	pb "ck_remote_proxy/protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"go.yaml.in/yaml/v3"
)

// Rewriter is responsible for rewriting Sentio-Network mode SQL into real SQL
type Rewriter interface {
	// Rewrite takes the original SQL and returns the rewritten SQL.
	// user and password are the client's credentials from Hello handshake,
	// used for remote() function calls.
	// If the SQL does not contain Sentio-Network mode table names, return the original SQL
	Rewrite(ctx context.Context, sql string, user string, password string) (string, error)
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
	// GetAllIndexerInfos returns all IndexerInfos in the network state.
	// Used by forwarding-only proxies to discover available targets.
	GetAllIndexerInfos() map[uint64]IndexerInfo
}

// Use sentio-core types directly (same field definitions)
type IndexerInfo = state.IndexerInfo
type ProcessorAllocation = state.ProcessorAllocation
type ProcessorInfo = state.ProcessorInfo

// RewriterConfig is the rewriter configuration
type RewriterConfig struct {
	Enabled     bool   // Whether to enable rewriting
	ServiceAddr string // sql-rewriter gRPC service address (required when enabled)
	Upstream    string // Upstream ClickHouse address (used for local/remote detection)
	Listen      string // Proxy listen address (e.g. ":9001"), used for remote() addr rewrite to localhost
	Timeout     time.Duration
}

// SentioNetworkRewriter implements the Rewriter interface
type SentioNetworkRewriter struct {
	config               RewriterConfig
	networkState         NetworkState
	grpcConn             *grpc.ClientConn
	grpcClient           pb.RewriterServiceClient // cached gRPC client stub
	tableRewriterFactory SentioNetworkTableRewriterFactory
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

// SentioNetworkTableRewriter encapsulates Sentio network table name mapping logic.
// Provided by @Chen, responsible for mapping virtual table names to physical table names.
type SentioNetworkTableRewriter interface {
	// Database returns the database name for this processor
	Database() string
	// RawTable maps a virtual table name to a physical table name; returns (physical_name, found, error)
	RawTable(table string) (string, bool, error)
	// RawTables batch-maps virtual table names to physical table names
	RawTables(tables ...string) (map[string]string, error)
	// All returns all table name mappings
	All() map[string]string
	// Reverse maps a physical table name back to a virtual table name
	Reverse(rawTable string) (string, bool, error)
}

// SentioNetworkTableRewriterFactory creates a SentioNetworkTableRewriter for a given processor.
type SentioNetworkTableRewriterFactory func(ctx context.Context, processorId string,
	indexerInfo IndexerInfo, processorInfo ProcessorInfo) (SentioNetworkTableRewriter, error)

// NewSentioNetworkRewriter creates a new SentioNetworkRewriter.
// The factory parameter provides a SentioNetworkTableRewriter for each processor.
// gRPC connection to sql-rewriter service is required (ServiceAddr must not be empty).
// Pass DefaultTableRewriterFactory() for backward-compatible simple prefix logic.
func NewSentioNetworkRewriter(config RewriterConfig, state NetworkState, factory SentioNetworkTableRewriterFactory) (*SentioNetworkRewriter, error) {
	if config.ServiceAddr == "" {
		return nil, fmt.Errorf("rewriter_service_addr is required when rewriter is enabled")
	}
	if factory == nil {
		factory = DefaultTableRewriterFactory("")
	}
	rewriter := &SentioNetworkRewriter{
		config:               config,
		networkState:         state,
		tableRewriterFactory: factory,
	}

	// Establish gRPC connection (blocking connect, ensures connection is ready before first RPC)
	// Add keepalive to maintain long connection health
	kaParams := keepalive.ClientParameters{
		Time:                30 * time.Second, // Send ping every 30s
		Timeout:             5 * time.Second,  // Ping timeout
		PermitWithoutStream: true,             // Keep pinging even without active streams
	}
	connectTimeout := config.Timeout
	if connectTimeout == 0 {
		connectTimeout = 10 * time.Second
	}
	connectCtx, connectCancel := context.WithTimeout(context.Background(), connectTimeout)
	defer connectCancel()

	conn, err := grpc.DialContext(connectCtx, config.ServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithBlock(), // Wait for connection to be established
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rewriter service at %s: %w", config.ServiceAddr, err)
	}
	rewriter.grpcConn = conn
	rewriter.grpcClient = pb.NewRewriterServiceClient(conn)
	log.Infof("connected to rewriter service at %s", config.ServiceAddr)

	return rewriter, nil
}

// Rewrite rewrites Sentio-Network mode SQL into real SQL.
//
// Two-phase approach (gRPC required):
//
//	Phase 1: Call sql-rewriter with empty options to get AST-parsed table names
//	         (original_accessed_table_names) — accurate, no false positives.
//	Phase 2: Filter for Sentio-Network pattern tables, build mappings via
//	         NetworkState + SentioNetworkTableRewriter, then call sql-rewriter
//	         again with the actual mappings to perform the rewrite.
func (r *SentioNetworkRewriter) Rewrite(ctx context.Context, sql string, user string, password string) (string, error) {
	// Phase 1: Get AST-parsed table names from sql-rewriter
	astTableNames, err := r.getASTTableNames(ctx, sql)
	if err != nil {
		return "", fmt.Errorf("phase 1 AST parsing failed: %w", err)
	}

	// Filter AST table names for Sentio-Network pattern
	tables := r.filterSentioNetworkTables(astTableNames)
	if len(tables) == 0 {
		return sql, nil
	}

	// Build rewrite mapping from NetworkState + SentioNetworkTableRewriter
	// user/password are from client Hello, used for remote() function calls
	tableWithDatabaseMap, remoteTableMap, err := r.buildRewriteMappings(ctx, tables, user, password)
	if err != nil {
		return "", err
	}

	// If no tables need rewriting, return original SQL
	if len(tableWithDatabaseMap) == 0 && len(remoteTableMap) == 0 {
		return sql, nil
	}

	// Phase 2: Call sql-rewriter service with actual mappings
	rewrittenSQL, err := r.callRewriterService(ctx, sql, tableWithDatabaseMap, remoteTableMap)
	if err != nil {
		return "", fmt.Errorf("phase 2 rewriter service call failed: %w", err)
	}
	return rewrittenSQL, nil
}

// getASTTableNames calls the sql-rewriter with empty options to get AST-parsed table names.
// The sql-rewriter (C++ ClickHouse parser) returns accurate table names via original_accessed_table_names.
func (r *SentioNetworkRewriter) getASTTableNames(ctx context.Context, sql string) ([]string, error) {
	timeout := r.config.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Send SQL with empty options — sql-rewriter will parse AST and return table names
	req := &pb.RewriteSQLRequest{
		Sql:     sql,
		Options: nil, // no rewrite options, just parse
	}

	resp, err := r.grpcClient.Rewrite(ctxWithTimeout, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}
	if resp.Code != pb.RewriteCode_Success {
		return nil, fmt.Errorf("rewriter parse error: %s", resp.Message)
	}

	log.Debugf("AST parsed table names: %v", resp.OriginalAccessedTableNames)
	return resp.OriginalAccessedTableNames, nil
}

// filterSentioNetworkTables filters AST-parsed table names for Sentio-Network pattern.
// Supports two formats:
//   - "sentio_<processorId>.<tableName>" — strips "sentio_" prefix to get processorId
//   - "<processorId>.<tableName>"        — uses database part directly as processorId
//
// Any "database.table" format name is accepted; NetworkState lookup will determine validity.
func (r *SentioNetworkRewriter) filterSentioNetworkTables(astTableNames []string) []ParsedTable {
	seen := make(map[string]bool)
	var tables []ParsedTable
	for _, name := range astTableNames {
		if seen[name] {
			continue
		}

		// Split by "." — expect "database.table" format
		parts := strings.SplitN(name, ".", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			continue
		}
		dbPart := parts[0]    // e.g. "sentio_coinbase" or "coinbase"
		tableName := parts[1] // e.g. "transfer"

		// Extract processorId: strip "sentio_" prefix if present
		processorId := dbPart
		const prefix = "sentio_"
		if strings.HasPrefix(strings.ToLower(dbPart), prefix) {
			processorId = dbPart[len(prefix):]
		}
		if processorId == "" {
			continue
		}

		seen[name] = true
		tables = append(tables, ParsedTable{
			FullMatch:   name,
			ProcessorId: processorId,
			TableName:   tableName,
		})
	}
	return tables
}

// buildRewriteMappings builds the local/remote table mappings from NetworkState and SentioNetworkTableRewriter.
// user and password are from client Hello, used for remote() function calls.
func (r *SentioNetworkRewriter) buildRewriteMappings(ctx context.Context, tables []ParsedTable, user string, password string) (map[string]TableWithDatabase, map[string]RemoteTable, error) {
	tableWithDatabaseMap := make(map[string]TableWithDatabase)
	remoteTableMap := make(map[string]RemoteTable)

	// Group tables by processorId to avoid redundant TableRewriter creation
	processorTables := make(map[string][]ParsedTable)
	for _, table := range tables {
		processorTables[table.ProcessorId] = append(processorTables[table.ProcessorId], table)
	}

	for processorId, pTables := range processorTables {
		// Get Processor allocation info — try processorId as-is first, then with "sentio_" prefix
		allocations, ok := r.networkState.GetProcessorAllocation(processorId)
		if !ok || len(allocations) == 0 {
			// Retry with "sentio_" prefix
			allocations, ok = r.networkState.GetProcessorAllocation("sentio_" + processorId)
			if !ok || len(allocations) == 0 {
				return nil, nil, fmt.Errorf("processor allocation not found for processor_id=%s (also tried sentio_%s)", processorId, processorId)
			}
		}

		// Take the first allocation (simplified handling)
		allocation := allocations[0]

		// Get Indexer info
		indexerInfo, ok := r.networkState.GetIndexerInfo(allocation.IndexerId)
		if !ok {
			return nil, nil, fmt.Errorf("indexer info not found for indexer_id=%d (processor_id=%s)", allocation.IndexerId, processorId)
		}

		// Get Processor info
		processorInfo, ok := r.networkState.GetProcessorInfo(processorId)
		if !ok {
			log.Warnf("processor info not found for processor_id=%s, using default", processorId)
			processorInfo = ProcessorInfo{ProcessorId: processorId}
		}

		// Create SentioNetworkTableRewriter for this processor
		tableRewriter, err := r.tableRewriterFactory(ctx, processorId, indexerInfo, processorInfo)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create table rewriter for processor_id=%s: %w", processorId, err)
		}
		database := tableRewriter.Database()

		// Determine local/remote by comparing IndexerInfo address with proxy's upstream
		indexerAddr := fmt.Sprintf("%s:%d", indexerInfo.IndexerUrl, indexerInfo.ClickhouseProxyPort)
		isLocal := indexerAddr == r.config.Upstream

		for _, table := range pTables {
			// Use SentioNetworkTableRewriter to resolve physical table name
			physicalTable, found, err := tableRewriter.RawTable(table.TableName)
			if err != nil {
				return nil, nil, fmt.Errorf("table rewriter RawTable failed for %s: %w", table.FullMatch, err)
			}
			if !found {
				return nil, nil, fmt.Errorf("table %s not found in rewriter mappings for processor_id=%s", table.FullMatch, processorId)
			}

			if isLocal {
				// Local table: use table_with_database_map
				tableWithDatabaseMap[table.FullMatch] = TableWithDatabase{
					Database: database,
					Table:    physicalTable,
				}
				log.Debugf("local table rewrite: %s -> %s.%s", table.FullMatch, database, physicalTable)
			} else {
				// Remote table: rewrite addr to localhost (back to local Proxy1)
				// and encode route info into user parameter for dynamic upstream routing.
				// Format: __route__<target_proxy_addr>__<real_user>
				localAddr := "localhost" + r.config.Listen // e.g. "localhost:9001"
				routeUser := fmt.Sprintf("__route__%s__%s", indexerAddr, user)
				remoteTableMap[table.FullMatch] = RemoteTable{
					Addr:     localAddr,
					Database: database,
					Table:    physicalTable,
					User:     routeUser,
					Password: password,
				}
				log.Debugf("remote table rewrite: %s -> remote('%s', '%s', '%s', '%s', '***')",
					table.FullMatch, localAddr, database, physicalTable, routeUser)
			}
		}
	}

	return tableWithDatabaseMap, remoteTableMap, nil
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

// DefaultTableRewriterFactory returns a default factory that maps logical to physical tables.
// Without CKHManager, this implementation passes the table name through directly.
func DefaultTableRewriterFactory(database string) SentioNetworkTableRewriterFactory {
	return func(ctx context.Context, processorId string,
		indexerInfo IndexerInfo, processorInfo ProcessorInfo) (SentioNetworkTableRewriter, error) {
		return &simpleTableRewriter{
			database:      database,
			processorId:   processorId,
			processorInfo: processorInfo,
		}, nil
	}
}

// simpleTableRewriter is the default TableRewriter implementation.
// It directly returns the table name without any physical mapping.
type simpleTableRewriter struct {
	database      string
	processorId   string
	processorInfo ProcessorInfo
}

func (s *simpleTableRewriter) Database() string {
	if s.database != "" {
		return s.database
	}
	return "sentio"
}

func (s *simpleTableRewriter) RawTable(table string) (string, bool, error) {
	// Without CKHManager, we assume the provided table name is already the physical table name.
	return table, true, nil
}

func (s *simpleTableRewriter) RawTables(tables ...string) (map[string]string, error) {
	result := make(map[string]string, len(tables))
	for _, t := range tables {
		raw, _, err := s.RawTable(t)
		if err != nil {
			return nil, err
		}
		result[t] = raw
	}
	return result, nil
}

func (s *simpleTableRewriter) All() map[string]string { return nil }

func (s *simpleTableRewriter) Reverse(rawTable string) (string, bool, error) {
	return "", false, nil
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

func (n NoopRewriter) Rewrite(ctx context.Context, sql string, user string, password string) (string, error) {
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

func (s *InMemoryNetworkState) GetAllIndexerInfos() map[uint64]IndexerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[uint64]IndexerInfo, len(s.IndexerInfos))
	for k, v := range s.IndexerInfos {
		cp[k] = v
	}
	return cp
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
