package sqlrouter

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"shardingSphere-go/config"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var dbPools = make(map[string]*sql.DB)
var dbMutex sync.RWMutex
var lastTime int64
var sequence int64
var mu sync.Mutex

// SQLResult carries execution information for both query and non-query statements.
type SQLResult struct {
	Columns      []string
	Rows         []map[string]interface{}
	Affected     int64
	LastInsertID int64
}

// RouteContext holds routing context for a SQL statement
type RouteContext struct {
	SQL           string
	TableName     string
	TableRule     config.TableRule
	DataNodes     interface{} // Either DataNode or DataNodeRange
	ParsedNodes   []config.DataNode
	ShardingValue interface{}
	DataSource    string
	Table         string
	SQLType       SQLType
	HasHint       bool
	HintDS        string
	HintTable     string
}

// RouteAndExecuteSQL routes based on sharding config and executes given SQL.
func RouteAndExecuteSQL(sql string) error {
	_, err := RouteAndExecuteSQLWithResult(sql)
	return err
}

// RouteAndExecuteSQLWithResult routes based on sharding config and executes given SQL.
// Returns a structured result.
func RouteAndExecuteSQLWithResult(sql string) (*SQLResult, error) {
	// Trim and validate
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, errors.New("empty SQL")
	}

	upperSQL := strings.ToUpper(sql)

	// Handle SHOW DATABASES specially - filter to only configured database and information_schema
	if strings.HasPrefix(upperSQL, "SHOW DATABASES") {
		return handleShowDatabases()
	}

	// Handle SHOW TABLES - only show configured sharding tables
	if strings.HasPrefix(upperSQL, "SHOW TABLES") {
		return handleShowTables()
	}

	// Handle USE database - only allow configured database
	if strings.HasPrefix(upperSQL, "USE ") {
		return handleUseDatabase(sql)
	}

	// Handle information_schema queries for database/table discovery
	if strings.Contains(upperSQL, "INFORMATION_SCHEMA.SCHEMATA") {
		return handleInfoSchemaDatabases()
	}
	if strings.Contains(upperSQL, "INFORMATION_SCHEMA.TABLES") {
		return handleInfoSchemaTables(sql)
	}
	if strings.Contains(upperSQL, "INFORMATION_SCHEMA.COLUMNS") {
		return handleInfoSchemaColumns(sql)
	}

	// Check if it's a system query that doesn't need sharding
	if config.IsSystemQuery(sql) {
		return routeToDefault(sql)
	}

	log.Printf("Routing SQL: %s", sql)

	// Parse SQL type
	sqlType := ParseSQLType(sql)

	// Extract table name
	parser := NewSQLParser(sql)
	tableName := parser.ExtractTableName()
	if tableName == "" {
		// Cannot extract table name, route to default datasource
		return routeToDefault(sql)
	}

	// Clean table name (remove backticks)
	cleanTableName := strings.Trim(tableName, "`")

	// Check for sharding hints first
	hasHint, hintDS, hintTable := checkShardingHints(sql)
	if hasHint {
		return routeWithHint(sql, cleanTableName, hintDS, hintTable)
	}

	// Get table rule
	rule, nodes, err := config.GetTableRuleForSQL(cleanTableName)
	if err != nil {
		// No sharding rule found, route to default
		return routeToDefault(sql)
	}

	// Check if it's a routed query (has sharding column in WHERE)
	ctx, err := buildRouteContext(sql, cleanTableName, rule, nodes)
	if err != nil {
		// Cannot route, execute on all nodes (broadcast) or default
		return routeToDefault(sql)
	}

	// Execute based on routing result
	return executeRoute(ctx, sqlType)
}

// checkShardingHints checks for sharding hints in SQL
func checkShardingHints(sql string) (bool, string, string) {
	hintDS, hasDS := GetHintDataSource(sql)
	hintTable, hasTable := GetHintTable(sql)
	return hasDS || hasTable, hintDS, hintTable
}

// routeWithHint routes SQL based on hint
func routeWithHint(sql, tableName, hintDS, hintTable string) (*SQLResult, error) {
	// Rewrite SQL with hint table if provided
	actualSQL := sql
	if hintTable != "" {
		actualSQL = RewriteTableInSQL(sql, hintTable)
	}

	// Route to specific data source
	db, err := getDBConnection(hintDS)
	if err != nil {
		return nil, err
	}

	return executeSQL(db, actualSQL)
}

// buildRouteContext builds the routing context
func buildRouteContext(sql, tableName string, rule config.TableRule, nodes interface{}) (*RouteContext, error) {
	ctx := &RouteContext{
		SQL:       sql,
		TableName: tableName,
		TableRule: rule,
		DataNodes: nodes,
	}

	// Determine if we need database routing
	needDBRoute := rule.DatabaseStrategy.Standard.ShardingColumn != ""
	needTableRoute := rule.TableStrategy.Standard.ShardingColumn != ""

	// Get sharding column for database
	var dbShardingColumn, tableShardingColumn string
	var dbShardingValue, tableShardingValue interface{}

	if needDBRoute {
		dbShardingColumn = rule.DatabaseStrategy.Standard.ShardingColumn
		parser := NewSQLParser(sql)
		val, err := parser.ExtractShardingValue(dbShardingColumn)
		if err != nil {
			return nil, fmt.Errorf("missing sharding value for column '%s': %v", dbShardingColumn, err)
		}
		dbShardingValue = val
		ctx.ShardingValue = val
	}

	if needTableRoute {
		tableShardingColumn = rule.TableStrategy.Standard.ShardingColumn
		if tableShardingColumn != dbShardingColumn {
			parser := NewSQLParser(sql)
			val, err := parser.ExtractShardingValue(tableShardingColumn)
			if err != nil {
				return nil, fmt.Errorf("missing sharding value for column '%s': %v", tableShardingColumn, err)
			}
			tableShardingValue = val
		} else {
			tableShardingValue = dbShardingValue
		}
	}

	// Calculate data source
	if needDBRoute {
		ds, err := calculateDataSource(rule.DatabaseStrategy.Standard.ShardingAlgorithmName, dbShardingValue)
		if err != nil {
			return nil, err
		}
		ctx.DataSource = ds
	}

	// Calculate table
	if needTableRoute {
		table, err := calculateTable(rule.TableStrategy.Standard.ShardingAlgorithmName, tableShardingValue)
		if err != nil {
			return nil, err
		}
		ctx.Table = table
	}

	// Parse and expand data nodes
	parsedNodes, err := expandDataNodes(nodes, ctx.DataSource, ctx.Table)
	if err != nil {
		return nil, err
	}
	ctx.ParsedNodes = parsedNodes

	return ctx, nil
}

// calculateDataSource calculates the target data source
func calculateDataSource(algorithmName string, shardingValue interface{}) (string, error) {
	if algorithmName == "" {
		return "", errors.New("no database sharding algorithm specified")
	}

	algorithm, err := config.GetShardingAlgorithm(algorithmName)
	if err != nil {
		return "", err
	}

	expression := algorithm.Props["algorithm-expression"]
	result, err := CalculateShardingExpression(expression, shardingValue)
	if err != nil {
		return "", err
	}

	// Extract data source name from expression
	// e.g., ds_${user_id % 2} -> ds_0 or ds_1
	modPattern := regexp.MustCompile(`\$\{(\w+)\s*%\s*(\d+)\}`)
	match := modPattern.FindStringSubmatch(expression)
	if match == nil {
		return expression, nil
	}

	_, _ = match[1], match[2] // Already have values
	modulo, _ := strconv.Atoi(match[2])

	var intValue int
	switch v := shardingValue.(type) {
	case int:
		intValue = v
	case int64:
		intValue = int(v)
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			intValue = i
		} else {
			intValue = hashString(v)
		}
	default:
		intValue = hashString(fmt.Sprintf("%v", v))
	}

	result = intValue % modulo
	return fmt.Sprintf("%s_%d", strings.TrimSuffix(strings.TrimSuffix(expression, match[0]), "$"), result), nil
}

// CalculateShardingExpression calculates the sharding expression result
func CalculateShardingExpression(expression string, shardingValue interface{}) (interface{}, error) {
	// Parse expression like "ds_${user_id % 2}" or "core_coin_logs_${user_id % 256}"
	modPattern := regexp.MustCompile(`\$\{(\w+)\s*%\s*(\d+)\}`)
	match := modPattern.FindStringSubmatch(expression)
	if match != nil {
		modulo, _ := strconv.Atoi(match[2])

		var intValue int
		switch v := shardingValue.(type) {
		case int:
			intValue = v
		case int64:
			intValue = int(v)
		case string:
			if i, err := strconv.Atoi(v); err == nil {
				intValue = i
			} else {
				intValue = hashString(v)
			}
		default:
			intValue = hashString(fmt.Sprintf("%v", v))
		}

		return intValue % modulo, nil
	}

	// Simple substitution without modulo
	simplePattern := regexp.MustCompile(`\$\{(\w+)\}`)
	return simplePattern.ReplaceAllStringFunc(expression, func(_ string) string {
		return fmt.Sprintf("%v", shardingValue)
	}), nil
}

// calculateTable calculates the target table
func calculateTable(algorithmName string, shardingValue interface{}) (string, error) {
	if algorithmName == "" {
		return "", nil
	}

	algorithm, err := config.GetShardingAlgorithm(algorithmName)
	if err != nil {
		return "", err
	}

	expression := algorithm.Props["algorithm-expression"]

	// Parse and calculate
	modPattern := regexp.MustCompile(`\$\{(\w+)\s*%\s*(\d+)\}`)
	match := modPattern.FindStringSubmatch(expression)
	if match == nil {
		return expression, nil
	}

	modulo, _ := strconv.Atoi(match[2])

	var intValue int
	switch v := shardingValue.(type) {
	case int:
		intValue = v
	case int64:
		intValue = int(v)
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			intValue = i
		} else {
			intValue = hashString(v)
		}
	default:
		intValue = hashString(fmt.Sprintf("%v", v))
	}

	result := intValue % modulo
	prefix := strings.TrimSuffix(strings.TrimSuffix(expression, match[0]), "$")
	return fmt.Sprintf("%s_%d", prefix, result), nil
}

// expandDataNodes expands data node range into concrete data nodes
func expandDataNodes(nodes interface{}, targetDS, targetTable string) ([]config.DataNode, error) {
	var result []config.DataNode

	switch n := nodes.(type) {
	case config.DataNode:
		result = append(result, n)
	case *config.DataNodeRange:
		rng := n
		if rng.TableRange[0] == rng.TableRange[1] && rng.TableRange[0] == 0 {
			// Database-only sharding
			if targetDS != "" {
				result = append(result, config.DataNode{
					DataSource: targetDS,
					Table:      rng.TablePattern,
				})
			} else {
				// Expand all data sources
				for ds := rng.DataSourceRange[0]; ds <= rng.DataSourceRange[1]; ds++ {
					result = append(result, config.DataNode{
						DataSource: fmt.Sprintf("%s_%d", rng.DataSourcePattern, ds),
						Table:      rng.TablePattern,
					})
				}
			}
		} else {
			// Database and table sharding
			dsList := []string{targetDS}
			if targetDS == "" {
				dsList = nil
				for ds := rng.DataSourceRange[0]; ds <= rng.DataSourceRange[1]; ds++ {
					dsList = append(dsList, fmt.Sprintf("%s_%d", rng.DataSourcePattern, ds))
				}
			}

			for _, ds := range dsList {
				if targetTable != "" {
					result = append(result, config.DataNode{
						DataSource: ds,
						Table:      targetTable,
					})
				} else {
					// Expand all tables
					for t := rng.TableRange[0]; t <= rng.TableRange[1]; t++ {
						result = append(result, config.DataNode{
							DataSource: ds,
							Table:      fmt.Sprintf("%s_%d", rng.TablePattern, t),
						})
					}
				}
			}
		}
	default:
		return nil, errors.New("invalid data nodes type")
	}

	return result, nil
}

// executeRoute executes SQL based on routing context
func executeRoute(ctx *RouteContext, sqlType SQLType) (*SQLResult, error) {
	// If no parsed nodes, execute on default
	if len(ctx.ParsedNodes) == 0 {
		return routeToDefault(ctx.SQL)
	}

	// Single node routing
	if len(ctx.ParsedNodes) == 1 {
		node := ctx.ParsedNodes[0]
		rewrittenSQL := RewriteTableInSQL(ctx.SQL, node.Table)

		db, err := getDBConnection(node.DataSource)
		if err != nil {
			return nil, err
		}

		return executeSQL(db, rewrittenSQL)
	}

	// Multi-node routing (e.g., full table scan or range query)
	return executeOnMultipleNodes(ctx, sqlType)
}

// executeOnMultipleNodes executes SQL on multiple data nodes
func executeOnMultipleNodes(ctx *RouteContext, sqlType SQLType) (*SQLResult, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make([]*SQLResult, 0, len(ctx.ParsedNodes))
	errors := make([]error, 0)

	for _, node := range ctx.ParsedNodes {
		wg.Add(1)
		go func(n config.DataNode) {
			defer wg.Done()

			rewrittenSQL := RewriteTableInSQL(ctx.SQL, n.Table)
			db, err := getDBConnection(n.DataSource)
			if err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
				return
			}

			result, err := executeSQL(db, rewrittenSQL)
			mu.Lock()
			if err != nil {
				errors = append(errors, err)
			} else if result != nil {
				results = append(results, result)
			}
			mu.Unlock()
		}(node)
	}

	wg.Wait()

	if len(errors) > 0 && len(results) == 0 {
		return nil, errors[0]
	}

	// Merge results for SELECT queries
	if sqlType == SQLTypeSelect {
		return mergeSelectResults(results), nil
	}

	// For DML, return the sum of affected rows
	if len(results) > 0 {
		var totalAffected int64
		var lastInsertID int64
		for _, r := range results {
			totalAffected += r.Affected
			if r.LastInsertID > 0 {
				lastInsertID = r.LastInsertID
			}
		}
		return &SQLResult{
			Affected:     totalAffected,
			LastInsertID: lastInsertID,
		}, nil
	}

	return &SQLResult{Affected: 0}, nil
}

// mergeSelectResults merges multiple SELECT results
func mergeSelectResults(results []*SQLResult) *SQLResult {
	if len(results) == 0 {
		return &SQLResult{}
	}

	if len(results) == 1 {
		return results[0]
	}

	// Use first result's columns
	merged := &SQLResult{
		Columns: results[0].Columns,
		Rows:    make([]map[string]interface{}, 0),
	}

	// Append all rows
	for _, r := range results {
		merged.Rows = append(merged.Rows, r.Rows...)
	}

	return merged
}

// routeToDefault routes to default data source
func routeToDefault(sql string) (*SQLResult, error) {
	dsName, err := config.GetDefaultDataSourceName()
	if err != nil {
		return nil, err
	}

	db, err := getDBConnection(dsName)
	if err != nil {
		return nil, err
	}

	return executeSQL(db, sql)
}

// getDBConnection gets or creates a database connection pool
func getDBConnection(targetDB string) (*sql.DB, error) {
	dbMutex.RLock()
	if db, exists := dbPools[targetDB]; exists {
		dbMutex.RUnlock()
		return db, nil
	}
	dbMutex.RUnlock()

	dbMutex.Lock()
	defer dbMutex.Unlock()

	// Double check after acquiring write lock
	if db, exists := dbPools[targetDB]; exists {
		return db, nil
	}

	// Create new database connection
	ds, err := config.GetDataSource(targetDB)
	if err != nil {
		return nil, err
	}

	dsn, err := config.BuildMySQLDSN(ds)
	if err != nil {
		return nil, err
	}

	dsn = sanitizeDSN(dsn)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Failed to open DB connection for %s: %v", targetDB, err)
		return nil, err
	}

	// Configure connection pool
	if ds.MaxPoolSize > 0 {
		db.SetMaxOpenConns(ds.MaxPoolSize)
	}
	if ds.MaxLifetimeMillis > 0 {
		db.SetConnMaxLifetime(time.Duration(ds.MaxLifetimeMillis) * time.Millisecond)
	}
	if ds.IdleTimeoutMillis > 0 {
		db.SetConnMaxIdleTime(time.Duration(ds.IdleTimeoutMillis) * time.Millisecond)
	}

	dbPools[targetDB] = db
	return db, nil
}

// executeSQL executes SQL and returns structured result
func executeSQL(db *sql.DB, sqlText string) (*SQLResult, error) {
	l := strings.ToLower(strings.TrimSpace(sqlText))
	isQuery := strings.HasPrefix(l, "select") ||
		strings.HasPrefix(l, "show") ||
		strings.HasPrefix(l, "describe") ||
		strings.HasPrefix(l, "explain")

	if isQuery {
		rows, err := db.Query(sqlText)
		if err != nil {
			log.Printf("SQL query error: %v", err)
			return nil, err
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		resultRows := make([]map[string]interface{}, 0, 16)
		for rows.Next() {
			values := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, err
			}
			rowMap := make(map[string]interface{}, len(cols))
			for i, c := range cols {
				v := values[i]
				if b, ok := v.([]byte); ok {
					rowMap[c] = string(b)
				} else {
					rowMap[c] = v
				}
			}
			resultRows = append(resultRows, rowMap)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}

		res := &SQLResult{
			Columns: cols,
			Rows:    resultRows,
		}
		return res, nil
	}

	// Non-query (INSERT/UPDATE/DELETE/DDL)
	r, err := db.Exec(sqlText)
	if err != nil {
		log.Printf("SQL execution error: %v", err)
		return nil, err
	}
	affected, _ := r.RowsAffected()
	lastID, _ := r.LastInsertId()

	res := &SQLResult{
		Affected:     affected,
		LastInsertID: lastID,
	}
	return res, nil
}

// sanitizeDSN removes unsupported params from DSN
func sanitizeDSN(dsn string) string {
	qIndex := strings.Index(dsn, "?")
	if qIndex == -1 {
		// Add multiStatements by default
		return dsn + "?multiStatements=true"
	}

	base := dsn[:qIndex]
	rawQuery := dsn[qIndex+1:]

	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return dsn
	}

	// Remove serverTimezone
	if tzVals, ok := values["serverTimezone"]; ok && len(tzVals) > 0 {
		tz := strings.ToUpper(tzVals[0])
		switch tz {
		case "UTC":
			values.Set("loc", "UTC")
		case "LOCAL":
			values.Set("loc", "Local")
		}
		values.Del("serverTimezone")
	}

	// Remove useSSL
	if sslVals, ok := values["useSSL"]; ok && len(sslVals) > 0 {
		ssl := strings.ToLower(strings.TrimSpace(sslVals[0]))
		if ssl == "true" {
			if _, tlsExists := values["tls"]; !tlsExists {
				values.Set("tls", "preferred")
			}
		}
		values.Del("useSSL")
	}

	// Enable multi-statements
	if _, ok := values["multiStatements"]; !ok {
		values.Set("multiStatements", "true")
	}

	newQuery := values.Encode()
	if newQuery == "" {
		return base
	}
	return base + "?" + newQuery
}

// IsQuery returns true if the SQL is expected to return a result set
func IsQuery(sqlText string) bool {
	l := strings.ToLower(strings.TrimSpace(sqlText))
	return strings.HasPrefix(l, "select") ||
		strings.HasPrefix(l, "show") ||
		strings.HasPrefix(l, "describe") ||
		strings.HasPrefix(l, "explain")
}

// currentTimeMillis returns current time in milliseconds
func currentTimeMillis() int64 {
	return (int64)(0) // Placeholder - actual implementation would use time.Now().UnixNano() / 1e6
}

// handleShowDatabases returns only the configured database and information_schema
func handleShowDatabases() (*SQLResult, error) {
	dbName := config.GetDatabaseName()
	rows := []map[string]interface{}{
		{"Database": "information_schema"},
		{"Database": dbName},
	}
	return &SQLResult{
		Columns: []string{"Database"},
		Rows:    rows,
	}, nil
}

// handleShowTables returns only configured sharding tables
func handleShowTables() (*SQLResult, error) {
	tables := config.GetAllShardingTables()
	rows := make([]map[string]interface{}, 0, len(tables))
	for _, t := range tables {
		rows = append(rows, map[string]interface{}{
			"Tables_in_" + config.GetDatabaseName(): t,
		})
	}
	return &SQLResult{
		Columns: []string{"Tables_in_" + config.GetDatabaseName()},
		Rows:    rows,
	}, nil
}

// handleInfoSchemaDatabases returns filtered SCHEMATA for information_schema queries
func handleInfoSchemaDatabases() (*SQLResult, error) {
	dbName := config.GetDatabaseName()
	rows := []map[string]interface{}{
		{"SCHEMA_NAME": "information_schema"},
		{"SCHEMA_NAME": dbName},
	}
	return &SQLResult{
		Columns: []string{"SCHEMA_NAME"},
		Rows:    rows,
	}, nil
}

// handleInfoSchemaTables returns filtered tables for information_schema queries
func handleInfoSchemaTables(_ string) (*SQLResult, error) {
	dbName := config.GetDatabaseName()
	tables := config.GetAllShardingTables()

	rows := make([]map[string]interface{}, 0)
	for _, t := range tables {
		rows = append(rows, map[string]interface{}{
			"TABLE_SCHEMA": dbName,
			"TABLE_NAME":   t,
			"TABLE_TYPE":   "BASE TABLE",
		})
	}

	return &SQLResult{
		Columns: []string{"TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE"},
		Rows:    rows,
	}, nil
}

// handleInfoSchemaColumns returns column info for configured tables
func handleInfoSchemaColumns(sql string) (*SQLResult, error) {
	// Return empty result for columns - let the actual query handle it
	return &SQLResult{
		Columns: []string{},
		Rows:    []map[string]interface{}{},
	}, nil
}

// handleUseDatabase validates and handles USE database commands
func handleUseDatabase(sql string) (*SQLResult, error) {
	parts := strings.SplitN(sql, " ", 2)
	if len(parts) < 2 {
		return nil, errors.New("invalid USE syntax")
	}
	dbName := strings.Trim(strings.TrimSpace(parts[1]), ";`")

	// Only allow configured database
	configuredDB := config.GetDatabaseName()
	if dbName != configuredDB && dbName != "information_schema" {
		return nil, fmt.Errorf("Unknown database '%s'", dbName)
	}

	// Return OK packet (affected rows = 0)
	return &SQLResult{
		Columns:      []string{},
		Rows:         []map[string]interface{}{},
		Affected:     0,
		LastInsertID: 0,
	}, nil
}
