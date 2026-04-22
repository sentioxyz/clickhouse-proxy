package proxy

import (
	"fmt"
	"regexp"
	"strings"
)

// showTablesRegex matches SHOW TABLES variants:
//
//	SHOW TABLES
//	SHOW FULL TABLES
//	SHOW TABLES FROM <db>
//	SHOW TABLES IN <db>
//	SHOW TABLES LIKE '<pattern>'
//	combinations of the above
var showTablesRegex = regexp.MustCompile(
	`(?i)^\s*SHOW\s+(?:FULL\s+)?TABLES(?:\s+(?:FROM|IN)\s+([^\s;]+))?(?:\s+LIKE\s+'[^']*')?\s*;?\s*$`)

// isShowTables returns true if the SQL is a SHOW TABLES statement.
func isShowTables(sql string) bool {
	return showTablesRegex.MatchString(strings.TrimSpace(sql))
}

// rewriteShowTablesWithProcessor rewrites a SHOW TABLES statement into a
// SELECT from system.tables with processor-prefix filtering.
//
// targetDB resolution order:
//  1. If the SQL contains FROM/IN <db>, that <db> takes priority.
//  2. Otherwise, fall back to the connection-level currentDB.
//
// Three cases after targetDB is resolved:
//
//  1. targetDB is empty (no USE executed, Hello carried no database, AND no FROM/IN):
//     → return "SELECT name FROM system.tables WHERE 1 = 0" (empty result set).
//     Security: do not leak table names without a known database context.
//
//  2. targetDB is found in dbProcessors (it is a processor database):
//     → return SELECT filtered by processorID prefix, with explicit database = '<targetDB>'.
//     IMPORTANT: we use an explicit database literal instead of currentDatabase()
//     because the FROM/IN clause may target a different database than the session's
//     current one, making currentDatabase() incorrect.
//
//  3. targetDB is not found in dbProcessors (e.g. "system", "default"):
//     → return "" as a sentinel meaning "no rewrite", caller passes through.
func rewriteShowTablesWithProcessor(sql, currentDB string, dbProcessors map[string]string) string {
	// Parse optional FROM/IN <db> override inside the SQL itself.
	targetDB := currentDB
	m := showTablesRegex.FindStringSubmatch(strings.TrimSpace(sql))
	if len(m) > 1 && m[1] != "" {
		targetDB = strings.Trim(m[1], "`\"")
	}

	// Case 1: No database context — return empty result set.
	if targetDB == "" {
		return "SELECT name FROM system.tables WHERE 1 = 0"
	}

	// Case 2: Database has a configured processor — apply prefix filter.
	// Use explicit database literal (not currentDatabase()) to correctly handle
	// SHOW TABLES FROM <other_db> where <other_db> != session's current database.
	if processorID, ok := dbProcessors[targetDB]; ok {
		// Use a subquery to filter rows first, then apply multiIf in the outer query.
		// ClickHouse allows SELECT aliases to be referenced in WHERE, so if both the
		// outer column and the alias are named "name", the WHERE clause would see the
		// aliased (already-stripped) value and filter incorrectly.
		return fmt.Sprintf(
			"SELECT multiIf(startsWith(name, '%[2]s_'), substring(name, length('%[2]s_') + 1), startsWith(name, '%[2]s.'), substring(name, length('%[2]s.') + 1), startsWith(name, '%[2]s'), substring(name, length('%[2]s') + 1), name) AS name FROM (SELECT name FROM system.tables WHERE database = '%[1]s' AND startsWith(name, '%[2]s'))",
			escapeSQLString(targetDB),
			escapeSQLString(processorID),
		)
	}

	// Case 3: Database not in config (system / default / non-processor DB).
	// Return "" as sentinel: caller will forward original SHOW TABLES unchanged.
	return ""
}

// escapeSQLString escapes single quotes for safe embedding in a SQL string literal.
// ClickHouse uses standard SQL escaping: ' → ''
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// escapeSQLLike escapes the special LIKE characters %, _ and \ in a pattern value.
// We must double-escape the backslash ('\\%', '\\_') because ClickHouse uses '\'
// as the string literal escape character. To pass '\_' to the LIKE engine, the
// SQL string literal must contain '\\_'.
func escapeSQLLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\\\`)
	s = strings.ReplaceAll(s, `%`, `\\%`)
	s = strings.ReplaceAll(s, `_`, `\\_`)
	return s
}
