package sqlrouter

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// SQLType represents the type of SQL statement
type SQLType int

const (
	SQLTypeUnknown SQLType = iota
	SQLTypeSelect
	SQLTypeInsert
	SQLTypeUpdate
	SQLTypeDelete
	SQLTypeDDL
	SQLTypeDML
)

// ParseSQLType parses SQL statement type
func ParseSQLType(sql string) SQLType {
	sql = strings.TrimSpace(strings.ToUpper(sql))

	if strings.HasPrefix(sql, "SELECT") {
		return SQLTypeSelect
	}
	if strings.HasPrefix(sql, "INSERT") {
		return SQLTypeInsert
	}
	if strings.HasPrefix(sql, "UPDATE") {
		return SQLTypeUpdate
	}
	if strings.HasPrefix(sql, "DELETE") {
		return SQLTypeDelete
	}
	if strings.HasPrefix(sql, "CREATE") ||
		strings.HasPrefix(sql, "ALTER") ||
		strings.HasPrefix(sql, "DROP") ||
		strings.HasPrefix(sql, "TRUNCATE") {
		return SQLTypeDDL
	}

	return SQLTypeDML
}

// SQLCondition represents a WHERE condition
type SQLCondition struct {
	Column    string
	Operator  string
	Value     string
	LogicOp   string // AND, OR
	Nested    bool
	NestedConditions []SQLCondition
}

// SQLParser parses SQL statements
type SQLParser struct {
	sql string
}

// NewSQLParser creates a new SQL parser
func NewSQLParser(sql string) *SQLParser {
	return &SQLParser{sql: sql}
}

// ExtractTableName extracts the table name from SQL
func (p *SQLParser) ExtractTableName() string {
	sql := strings.TrimSpace(p.sql)
	sqlUpper := strings.ToUpper(sql)

	// SELECT ... FROM table
	if strings.HasPrefix(sqlUpper, "SELECT") {
		fromIdx := strings.Index(sqlUpper, " FROM ")
		if fromIdx != -1 {
			rest := strings.TrimSpace(sql[fromIdx+6:])
			return extractFirstIdentifier(rest)
		}
		// Handle JOIN
		joinIdx := strings.Index(sqlUpper, " JOIN ")
		if joinIdx != -1 {
			rest := strings.TrimSpace(sql[joinIdx+6:])
			return extractFirstIdentifier(rest)
		}
	}

	// INSERT INTO table
	if strings.HasPrefix(sqlUpper, "INSERT INTO") {
		rest := strings.TrimSpace(sql[11:])
		return extractFirstIdentifier(rest)
	}

	// UPDATE table
	if strings.HasPrefix(sqlUpper, "UPDATE ") {
		rest := strings.TrimSpace(sql[7:])
		return extractFirstIdentifier(rest)
	}

	// DELETE FROM table
	if strings.HasPrefix(sqlUpper, "DELETE FROM ") {
		rest := strings.TrimSpace(sql[12:])
		return extractFirstIdentifier(rest)
	}

	return ""
}

// ExtractShardingCondition extracts sharding conditions from WHERE clause
func (p *SQLParser) ExtractShardingCondition(shardingColumn string) (string, error) {
	sql := strings.TrimSpace(p.sql)
	sqlUpper := strings.ToUpper(sql)

	whereIdx := strings.Index(sqlUpper, "WHERE")
	if whereIdx == -1 {
		return "", errors.New("no WHERE clause found")
	}

	whereClause := sql[whereIdx+5:]
	
	// Find the end of WHERE clause (ORDER BY, GROUP BY, LIMIT, or end)
	endIdx := len(whereClause)
	for _, keyword := range []string{"ORDER BY", "GROUP BY", "LIMIT", "HAVING"} {
		if idx := strings.Index(strings.ToUpper(whereClause), keyword); idx != -1 && idx < endIdx {
			endIdx = idx
		}
	}
	whereClause = strings.TrimSpace(whereClause[:endIdx])

	// Find the sharding column condition
	pattern := regexp.MustCompile(fmt.Sprintf(`(?i)\b%s\s*(=|!=|<>|<|>|>=|<=|IN\s*\(|BETWEEN\s+)`, regexp.QuoteMeta(shardingColumn)))
	match := pattern.FindStringSubmatch(whereClause)
	if match == nil {
		return "", errors.New("sharding column not found in WHERE clause")
	}

	// Extract the full condition
	condStart := pattern.FindStringIndex(whereClause)
	if condStart == nil {
		return "", errors.New("cannot parse condition")
	}

	condEnd := len(whereClause)
	for _, keyword := range []string{"AND", "OR"} {
		keywordIdx := strings.Index(strings.ToUpper(whereClause[condStart[1]:]), " "+keyword+" ")
		if keywordIdx != -1 {
			if condStart[1]+keywordIdx < condEnd {
				condEnd = condStart[1] + keywordIdx
			}
		}
	}

	condition := strings.TrimSpace(whereClause[condStart[0]:condEnd])
	return condition, nil
}

// ExtractShardingValue extracts the sharding value from a condition
func (p *SQLParser) ExtractShardingValue(shardingColumn string) (interface{}, error) {
	condition, err := p.ExtractShardingCondition(shardingColumn)
	if err != nil {
		return nil, err
	}

	// Remove the column and operator
	pattern := regexp.MustCompile(fmt.Sprintf(`(?i)\b%s\s*(=|!=|<>|<|>|>=|<=|IN\s*\(|BETWEEN\s+)`, regexp.QuoteMeta(shardingColumn)))
	remainder := pattern.ReplaceAllString(condition, "")
	remainder = strings.TrimSpace(remainder)

	// Handle IN clause
	if strings.HasPrefix(strings.ToUpper(remainder), "IN") {
		remainder = strings.TrimPrefix(remainder, "IN")
		remainder = strings.TrimSpace(remainder)
		remainder = strings.Trim(remainder, "() ")
		values := strings.Split(remainder, ",")
		if len(values) > 0 {
			return strings.Trim(strings.TrimSpace(values[0]), "'\"`"), nil
		}
	}

	// Handle BETWEEN clause
	if strings.HasPrefix(strings.ToUpper(remainder), "BETWEEN") {
		remainder = strings.TrimPrefix(remainder, "BETWEEN")
		remainder = strings.TrimSpace(remainder)
		parts := strings.Split(remainder, "AND")
		if len(parts) > 0 {
			return strings.Trim(strings.TrimSpace(parts[0]), "'\"`"), nil
		}
	}

	// Simple value
	value := strings.Trim(remainder, "'\"` ")
	return value, nil
}

// extractFirstIdentifier extracts the first identifier from a string
func extractFirstIdentifier(s string) string {
	s = strings.TrimSpace(s)

	// Handle backtick quoted identifiers
	if strings.HasPrefix(s, "`") {
		if end := strings.Index(s[1:], "`"); end > 0 {
			return s[1 : end+1]
		}
	}

	// Handle quoted identifiers
	if strings.HasPrefix(s, "\"") {
		if end := strings.Index(s[1:], "\""); end > 0 {
			return s[1 : end+1]
		}
	}

	// Handle single-quoted string value as identifier (edge case)
	if strings.HasPrefix(s, "'") {
		return s
	}

	// Split by common delimiters
	fields := strings.Fields(s)
	if len(fields) > 0 {
		name := fields[0]
		name = strings.TrimRight(name, ",")
		if idx := strings.Index(name, "."); idx != -1 {
			name = name[idx+1:]
		}
		name = strings.Trim(name, "`")
		return name
	}

	return s
}

// RewriteTableName rewrites the table name in SQL
func RewriteTableName(sql, oldTable, newTable string) string {
	// Handle backtick quoted table names
	sql = regexp.MustCompile(fmt.Sprintf("`%s`", regexp.QuoteMeta(oldTable))).
		ReplaceAllString(sql, fmt.Sprintf("`%s`", newTable))
	
	// Handle unquoted table names
	sql = regexp.MustCompile(fmt.Sprintf(`\b%s\b`, regexp.QuoteMeta(oldTable))).
		ReplaceAllString(sql, newTable)

	return sql
}

// RewriteTableInSQL rewrites all table references in SQL
func RewriteTableInSQL(sql, newTableName string) string {
	parser := NewSQLParser(sql)
	tableName := parser.ExtractTableName()
	
	if tableName == "" {
		return sql
	}

	return RewriteTableName(sql, tableName, newTableName)
}

// BuildSQLWithHint builds SQL with hint (for forced routing)
func BuildSQLWithHint(sql, hint string) string {
	return fmt.Sprintf("/* %s */ %s", hint, sql)
}

// ParseShardingHints parses sharding hints from SQL comments
func ParseShardingHints(sql string) map[string]string {
	hints := make(map[string]string)
	
	// Find /* ... */ hints
	hintPattern := regexp.MustCompile(`/\*\s*(?:shardingSphere:)?(\w+)=([^\s*]+)\s*\*/`)
	matches := hintPattern.FindAllStringSubmatch(sql, -1)
	
	for _, match := range matches {
		hints[match[1]] = match[2]
	}
	
	return hints
}

// IsShardingHintPresent checks if sharding hints are present
func IsShardingHintPresent(sql string) bool {
	return strings.Contains(sql, "/* shardingSphere:") || 
	       strings.Contains(sql, "/*:ds=") ||
	       strings.Contains(sql, "/*:tb=")
}

// GetHintDataSource extracts data source hint
func GetHintDataSource(sql string) (string, bool) {
	pattern := regexp.MustCompile(`/\*\s*(?:shardingSphere:)?(?:ds|datasource)=([^\s*]+)\s*\*/`)
	match := pattern.FindStringSubmatch(sql)
	if match != nil {
		return match[1], true
	}
	return "", false
}

// GetHintTable extracts table hint
func GetHintTable(sql string) (string, bool) {
	pattern := regexp.MustCompile(`/\*\s*(?:shardingSphere:)?(?:tb|table)=([^\s*]+)\s*\*/`)
	match := pattern.FindStringSubmatch(sql)
	if match != nil {
		return match[1], true
	}
	return "", false
}
