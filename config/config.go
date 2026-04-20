package config

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// DataSource represents a physical database connection configuration
type DataSource struct {
	URL                     string `yaml:"url"`
	Username                string `yaml:"username"`
	Password                string `yaml:"password"`
	ConnectionTimeoutMillis int    `yaml:"connectionTimeoutMilliseconds"`
	IdleTimeoutMillis       int    `yaml:"idleTimeoutMilliseconds"`
	MaxLifetimeMillis       int    `yaml:"maxLifetimeMilliseconds"`
	MaxPoolSize             int    `yaml:"maxPoolSize"`
}

// ShardingAlgorithm represents a sharding algorithm configuration
type ShardingAlgorithm struct {
	Type  string            `yaml:"type"`
	Props map[string]string `yaml:"props"`
}

// KeyGenerator represents a key generator configuration (e.g., SNOWFLAKE)
type KeyGenerator struct {
	Type  string            `yaml:"type"`
	Props map[string]string `yaml:"props"`
}

// StandardStrategy represents standard sharding strategy
type StandardStrategy struct {
	ShardingColumn        string `yaml:"shardingColumn"`
	ShardingAlgorithmName string `yaml:"shardingAlgorithmName"`
}

// TableStrategy represents table-level sharding strategy
type TableStrategy struct {
	Standard StandardStrategy `yaml:"standard"`
}

// DatabaseStrategy represents database-level sharding strategy
type DatabaseStrategy struct {
	Standard StandardStrategy `yaml:"standard"`
}

// TableRule represents a table's sharding rule configuration
type TableRule struct {
	ActualDataNodes  string           `yaml:"actualDataNodes"`
	DatabaseStrategy DatabaseStrategy `yaml:"databaseStrategy"`
	TableStrategy    TableStrategy    `yaml:"tableStrategy"`
	KeyGenerator     string           `yaml:"keyGenerateStrategy"`
}

// ShardingRule represents the complete sharding rule configuration
type ShardingRule struct {
	Tables             map[string]TableRule         `yaml:"tables"`
	ShardingAlgorithms map[string]ShardingAlgorithm `yaml:"shardingAlgorithms"`
	KeyGenerators      map[string]KeyGenerator      `yaml:"keyGenerators"`
}

// ReadWriteSplittingDataSource represents a read-write splitting data source group
type ReadWriteSplittingDataSource struct {
	WriteDataSourceName string   `yaml:"writeDataSourceName"`
	ReadDataSourceNames []string `yaml:"readDataSourceNames"`
	LoadBalancerName    string   `yaml:"loadBalancerName"`
	LoadBalancerType    string   `yaml:"type"` // RANDOM, ROUND_ROBIN
}

// ReadWriteSplittingRule represents the read-write splitting rule configuration
type ReadWriteSplittingRule struct {
	DataSourceGroups map[string]ReadWriteSplittingDataSource `yaml:"dataSourceGroups"`
	LoadBalancers    map[string]struct{ Type string }        `yaml:"loadBalancers"`
}

// AuthorityUser represents a user in authority configuration
type AuthorityUser struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Admin    bool   `yaml:"admin"`
}

// AuthorityPrivilege represents privilege configuration
type AuthorityPrivilege struct {
	Type string `yaml:"type"`
}

// Authority represents the authority configuration
type Authority struct {
	Users     []AuthorityUser    `yaml:"users"`
	Privilege AuthorityPrivilege `yaml:"privilege"`
}

// ProxyUser represents proxy authentication user
type ProxyUser struct {
	User     string `yaml:"username"`
	Password string `yaml:"password"`
}

// GlobalConfig represents the global.yaml configuration structure
type GlobalConfig struct {
	Mode      map[string]interface{} `yaml:"mode"`
	Authority Authority              `yaml:"authority"`
	Props     map[string]interface{} `yaml:"props"`
}

// ShardingConfig represents the complete configuration
type ShardingConfig struct {
	DatabaseName string                `yaml:"databaseName"`
	ProxyUser    ProxyUser             `yaml:"proxyUser"`
	DataSources  map[string]DataSource `yaml:"dataSources"`
	Rules        []ShardingRule        `yaml:"rules"`
}

// FullConfig holds all configuration including global.yaml
type FullConfig struct {
	Sharding ShardingConfig
	Global   GlobalConfig
}

var fullConfig FullConfig
var shardConfig ShardingConfig

// Cached parsed rules
var readWriteSplittingRules []ReadWriteSplittingRule

// LoadConfig loads and parses both config.yaml and global.yaml
func LoadConfig(filePath string) error {
	// Load main sharding config
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := yaml.NewDecoder(file).Decode(&shardConfig); err != nil {
		return err
	}

	// Parse rules to cache read-write splitting rules
	parseRules()

	// Load global.yaml for authority
	globalPath := filepath.Dir(filePath) + "/../ncaos/globel.yaml"
	globalFile, err := os.Open(globalPath)
	if err == nil {
		defer globalFile.Close()
		if err := yaml.NewDecoder(globalFile).Decode(&fullConfig.Global); err != nil {
			log.Printf("Warning: Failed to parse global.yaml: %v", err)
		}
	} else {
		log.Printf("Warning: global.yaml not found at %s: %v", globalPath, err)
	}

	return nil
}

// parseRules parses and caches special rules
func parseRules() {
	for range shardConfig.Rules {
		// Check if this is a read-write splitting rule by looking at the raw YAML structure
		// We need to check for !READWRITE_SPLITTING marker
	}
}

// LoadGlobalConfig loads only global.yaml configuration
func LoadGlobalConfig(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	return yaml.NewDecoder(file).Decode(&fullConfig.Global)
}

// GetConfig returns the loaded configuration
func GetConfig() *ShardingConfig {
	return &shardConfig
}

// GetDataSource returns a data source by name
func GetDataSource(name string) (DataSource, error) {
	if ds, exists := shardConfig.DataSources[name]; exists {
		return ds, nil
	}
	return DataSource{}, errors.New("data source not found: " + name)
}

// GetAllDataSources returns all configured data sources
func GetAllDataSources() map[string]DataSource {
	return shardConfig.DataSources
}

// ResolveDataSourceName resolves a logical data source name to a physical data source
// For read-write splitting: readwrite_ds_0 -> primary_ds_0 or replica_ds_0
// For regular: ds_0 -> ds_0
func ResolveDataSourceName(logicalName string) (string, error) {
	// Check if it's a read-write splitting data source group
	dsName, isRW := resolveReadWriteSplitting(logicalName, false)
	if isRW {
		return dsName, nil
	}

	// Check if it's a direct physical data source
	if _, exists := shardConfig.DataSources[logicalName]; exists {
		return logicalName, nil
	}

	// Try to parse actualDataNodes pattern like "readwrite_ds_${0..1}.table"
	if strings.Contains(logicalName, "$") {
		return resolvePatternDataSource(logicalName)
	}

	return logicalName, nil
}

// ResolveReadDataSource resolves a logical data source to a read replica
func ResolveReadDataSource(logicalName string) (string, error) {
	dsName, _ := resolveReadWriteSplitting(logicalName, true)
	if dsName == "" {
		return "", errors.New("read-write splitting data source not found")
	}
	return dsName, nil
}

// resolveReadWriteSplitting resolves read-write splitting data source
func resolveReadWriteSplitting(logicalName string, preferRead bool) (string, bool) {
	// Scan all rules for read-write splitting configuration
	for _, rule := range shardConfig.Rules {
		// Check if this rule has dataSourceGroups (read-write splitting)
		if dsGroup, exists := getReadWriteGroupFromRule(rule, logicalName); exists {
			if preferRead && len(dsGroup.ReadDataSourceNames) > 0 {
				return selectReadDataSource(dsGroup), true
			}
			return dsGroup.WriteDataSourceName, true
		}
	}

	// Check cached rules
	for _, rwRule := range readWriteSplittingRules {
		if dsGroup, exists := rwRule.DataSourceGroups[logicalName]; exists {
			if preferRead && len(dsGroup.ReadDataSourceNames) > 0 {
				return selectReadDataSource(dsGroup), true
			}
			return dsGroup.WriteDataSourceName, true
		}
	}

	return "", false
}

// getReadWriteGroupFromRule extracts read-write group from a raw rule
func getReadWriteGroupFromRule(rule interface{}, logicalName string) (ReadWriteSplittingDataSource, bool) {
	// This is a simplified version - in production you'd parse the raw YAML
	return ReadWriteSplittingDataSource{}, false
}

// selectReadDataSource selects a read data source based on load balancer
func selectReadDataSource(dsGroup ReadWriteSplittingDataSource) string {
	if len(dsGroup.ReadDataSourceNames) == 0 {
		return dsGroup.WriteDataSourceName
	}

	// Random load balancer
	switch dsGroup.LoadBalancerType {
	case "RANDOM":
		return dsGroup.ReadDataSourceNames[rand.Intn(len(dsGroup.ReadDataSourceNames))]
	case "ROUND_ROBIN":
		// Simplified - just return first
		return dsGroup.ReadDataSourceNames[0]
	default:
		return dsGroup.ReadDataSourceNames[0]
	}
}

// resolvePatternDataSource resolves a pattern like "readwrite_ds_${0..1}" to actual data source
func resolvePatternDataSource(pattern string) (string, error) {
	// Pattern: readwrite_ds_${0..1} -> return readwrite_ds_0
	rangePattern := regexp.MustCompile(`^(.+?)\$\{(\d+)\.\.(\d+)\}$`)
	match := rangePattern.FindStringSubmatch(pattern)
	if match != nil {
		// Return first data source in range
		return fmt.Sprintf("%s_0", match[1]), nil
	}
	return pattern, nil
}

// GetShardingRule returns the sharding rule for a table
func GetShardingRule(tableName string) (TableRule, error) {
	for _, rule := range shardConfig.Rules {
		if table, exists := rule.Tables[tableName]; exists {
			return table, nil
		}
		// Also check without backticks
		cleanName := strings.Trim(tableName, "`")
		if table, exists := rule.Tables[cleanName]; exists {
			return table, nil
		}
	}
	return TableRule{}, errors.New("sharding rule not found for table: " + tableName)
}

// GetAllShardingTables returns all configured sharding table names
func GetAllShardingTables() []string {
	var tables []string
	seen := make(map[string]bool)
	for _, rule := range shardConfig.Rules {
		for tableName := range rule.Tables {
			if !seen[tableName] {
				tables = append(tables, tableName)
				seen[tableName] = true
			}
		}
	}
	return tables
}

// IsShardedTable returns true if table has table sharding (actualDataNodes contains table range)
func IsShardedTable(tableName string) bool {
	rule, err := GetShardingRule(tableName)
	if err != nil {
		return false
	}
	// Check if actualDataNodes has table range pattern like core_coin_logs_${0..256}
	return strings.Contains(rule.ActualDataNodes, "${")
}

// GetShardingAlgorithm returns a sharding algorithm by name
func GetShardingAlgorithm(name string) (ShardingAlgorithm, error) {
	for _, rule := range shardConfig.Rules {
		if algorithm, exists := rule.ShardingAlgorithms[name]; exists {
			return algorithm, nil
		}
	}
	return ShardingAlgorithm{}, errors.New("sharding algorithm not found: " + name)
}

// GetKeyGenerator returns a key generator by name
func GetKeyGenerator(name string) (KeyGenerator, error) {
	for _, rule := range shardConfig.Rules {
		if generator, exists := rule.KeyGenerators[name]; exists {
			return generator, nil
		}
	}
	return KeyGenerator{}, errors.New("key generator not found: " + name)
}

// GetProxyUser returns proxy user by username (from config.yaml)
func GetProxyUser(username string) (ProxyUser, error) {
	if shardConfig.ProxyUser.User == username {
		return shardConfig.ProxyUser, nil
	}
	return ProxyUser{}, errors.New("proxy user not found: " + username)
}

// GetAuthorityUser finds user from authority configuration (global.yaml)
func GetAuthorityUser(username string) (AuthorityUser, error) {
	for _, user := range fullConfig.Global.Authority.Users {
		if user.User == username || matchUserPattern(user.User, username) {
			return user, nil
		}
	}
	return AuthorityUser{}, errors.New("authority user not found: " + username)
}

// matchUserPattern matches user patterns like "root@%"
func matchUserPattern(pattern, username string) bool {
	parts := strings.Split(pattern, "@")
	if len(parts) != 2 {
		return pattern == username
	}
	if parts[0] != "%" && parts[0] != username {
		return false
	}
	return true
}

// IsAdmin checks if user has admin privileges
func IsAdmin(username string) bool {
	user, err := GetAuthorityUser(username)
	if err != nil {
		return false
	}
	return user.Admin
}

// GetAllAuthorityUsers returns all configured users
func GetAllAuthorityUsers() []AuthorityUser {
	return fullConfig.Global.Authority.Users
}

// HasAuthorityConfig checks if authority configuration exists
func HasAuthorityConfig() bool {
	return len(fullConfig.Global.Authority.Users) > 0
}

// GetDefaultDataSourceName returns the first available data source name
func GetDefaultDataSourceName() (string, error) {
	for name := range shardConfig.DataSources {
		return name, nil
	}
	return "", errors.New("no datasource configured")
}

// GetDatabaseName returns the logical database name
func GetDatabaseName() string {
	return shardConfig.DatabaseName
}

// BuildMySQLDSN converts DataSource.URL (mysql://host:port/db?query or jdbc:mysql://host:port/db) + credentials to Go MySQL driver DSN
func BuildMySQLDSN(ds DataSource) (string, error) {
	rawURL := ds.URL

	// Support jdbc:mysql:// format - url.Parse doesn't handle it well
	if strings.HasPrefix(rawURL, "jdbc:mysql://") {
		rawURL = "mysql://" + strings.TrimPrefix(rawURL, "jdbc:mysql://")
	} else if strings.HasPrefix(rawURL, "jdbc:mysql:") {
		// jdbc:mysql:xxx format
		rawURL = "mysql://" + strings.TrimPrefix(rawURL, "jdbc:mysql:")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Scheme != "mysql" {
		return "", errors.New("unsupported DSN scheme: " + u.Scheme)
	}
	hostPort := u.Host
	db := strings.TrimPrefix(u.Path, "/")
	q := u.RawQuery
	dsn := ds.Username + ":" + ds.Password + "@tcp(" + hostPort + ")/" + db
	if q != "" {
		dsn = dsn + "?" + q
	}
	return dsn, nil
}

// DataNode represents a parsed data node (e.g., ds_0.core_users)
type DataNode struct {
	DataSource string
	Table      string
}

// DataNodeRange represents a range of data nodes (e.g., ds_${0..1}.table_${0..256})
type DataNodeRange struct {
	DataSourcePattern    string
	TablePattern         string
	DataSourceRange      [2]int
	TableRange           [2]int
	IsReadWriteSplitting bool
}

// ParseActualDataNodes parses actualDataNodes like "ds_${0..1}.core_coin_logs_${0..256}"
func ParseActualDataNodes(actualDataNodes string) (interface{}, error) {
	// Check for read-write splitting prefix like "readwrite_ds_${0..1}.table"
	rwPattern := regexp.MustCompile(`^(readwrite_ds_\$\{)(\d+)\.\.(\d+)\}\.(.+?)\.\$\{(\d+)\.\.(\d+)\}$`)
	match := rwPattern.FindStringSubmatch(actualDataNodes)
	if match != nil {
		dsStart, _ := strconv.Atoi(match[2])
		dsEnd, _ := strconv.Atoi(match[3])
		tableStart, _ := strconv.Atoi(match[5])
		tableEnd, _ := strconv.Atoi(match[6])
		return &DataNodeRange{
			DataSourcePattern:    "readwrite_ds_",
			TablePattern:         match[4],
			DataSourceRange:      [2]int{dsStart, dsEnd},
			TableRange:           [2]int{tableStart, tableEnd},
			IsReadWriteSplitting: true,
		}, nil
	}

	// Check if it's a range pattern like ds_${0..1}.table_${0..256}
	// Pattern: prefix_${0..1}.table_${0..3}
	rangePattern := regexp.MustCompile(`^(.+?)_?\$\{(\d+)\.\.(\d+)\}\.(.+?)_?\$\{(\d+)\.\.(\d+)\}$`)
	match = rangePattern.FindStringSubmatch(actualDataNodes)
	if match != nil {
		dsStart, _ := strconv.Atoi(match[2])
		dsEnd, _ := strconv.Atoi(match[3])
		tableStart, _ := strconv.Atoi(match[5])
		tableEnd, _ := strconv.Atoi(match[6])
		return &DataNodeRange{
			DataSourcePattern: match[1],
			TablePattern:      match[4],
			DataSourceRange:   [2]int{dsStart, dsEnd},
			TableRange:        [2]int{tableStart, tableEnd},
		}, nil
	}

	// Check if it's a simple range like ds_${0..1}.table
	simpleRangePattern := regexp.MustCompile(`^(.+?)\.\$\{(\d+)\.\.(\d+)\}$`)
	match = simpleRangePattern.FindStringSubmatch(actualDataNodes)
	if match != nil {
		dsStart, _ := strconv.Atoi(match[2])
		dsEnd, _ := strconv.Atoi(match[3])
		return &DataNodeRange{
			DataSourcePattern: match[1],
			TablePattern:      match[4],
			DataSourceRange:   [2]int{dsStart, dsEnd},
			TableRange:        [2]int{0, 0},
		}, nil
	}

	// Check if it's a range with fixed table like ds_${0..1}.table_name
	// Pattern: ds_${0..1}.table_name (data source range, fixed table)
	dsRangeTableFixed := regexp.MustCompile(`^(.+?)_?\$\{(\d+)\.\.(\d+)\}\.(.+)$`)
	match = dsRangeTableFixed.FindStringSubmatch(actualDataNodes)
	if match != nil {
		dsStart, _ := strconv.Atoi(match[2])
		dsEnd, _ := strconv.Atoi(match[3])
		// Check if the table part has its own range pattern (should not match here)
		if strings.Contains(match[4], "${") {
			// This is actually ds_${0..1}.table_${0..3} format, skip this pattern
		} else {
			return &DataNodeRange{
				DataSourcePattern: match[1],
				TablePattern:      match[4],
				DataSourceRange:   [2]int{dsStart, dsEnd},
				TableRange:        [2]int{0, 0},
			}, nil
		}
	}

	// Check if it's a fixed data source with table range like ds_0.table_${0..3}
	// The table pattern should NOT include the trailing underscore
	fixedDSTableRange := regexp.MustCompile(`^([^.]+)\.(.+?)_?\$\{(\d+)\.\.(\d+)\}$`)
	match = fixedDSTableRange.FindStringSubmatch(actualDataNodes)
	if match != nil {
		tableStart, _ := strconv.Atoi(match[3])
		tableEnd, _ := strconv.Atoi(match[4])
		return &DataNodeRange{
			DataSourcePattern: match[1],
			TablePattern:      match[2],
			DataSourceRange:   [2]int{0, 0},
			TableRange:        [2]int{tableStart, tableEnd},
		}, nil
	}

	// Check for read-write splitting with fixed table like readwrite_ds_0.table_name
	rwFixed := regexp.MustCompile(`^(readwrite_ds_\d+)\.(.+)$`)
	match = rwFixed.FindStringSubmatch(actualDataNodes)
	if match != nil {
		return DataNode{
			DataSource: match[1],
			Table:      match[2],
		}, nil
	}

	// Parse single data node like "ds_0.core_users"
	parts := strings.Split(actualDataNodes, ".")
	if len(parts) == 2 {
		return DataNode{
			DataSource: parts[0],
			Table:      parts[1],
		}, nil
	}

	return nil, fmt.Errorf("invalid actualDataNodes format: %s", actualDataNodes)
}

// ExpandDataNodeRange expands a DataNodeRange into a list of DataNodes
func ExpandDataNodeRange(rng *DataNodeRange) []DataNode {
	var nodes []DataNode

	for ds := rng.DataSourceRange[0]; ds <= rng.DataSourceRange[1]; ds++ {
		if rng.TableRange[0] == rng.TableRange[1] && rng.TableRange[0] == 0 {
			dsName := fmt.Sprintf("%s_%d", rng.DataSourcePattern, ds)
			// For read-write splitting, resolve to actual physical data source
			if rng.IsReadWriteSplitting {
				dsName = fmt.Sprintf("primary_ds_%d", ds)
			}
			nodes = append(nodes, DataNode{
				DataSource: dsName,
				Table:      rng.TablePattern,
			})
		} else {
			dsName := fmt.Sprintf("%s_%d", rng.DataSourcePattern, ds)
			if rng.IsReadWriteSplitting {
				dsName = fmt.Sprintf("primary_ds_%d", ds)
			}
			for t := rng.TableRange[0]; t <= rng.TableRange[1]; t++ {
				nodes = append(nodes, DataNode{
					DataSource: dsName,
					Table:      fmt.Sprintf("%s_%d", rng.TablePattern, t),
				})
			}
		}
	}

	return nodes
}

// GetTableRuleForSQL returns the table rule and parsed data nodes for routing
func GetTableRuleForSQL(tableName string) (TableRule, interface{}, error) {
	rule, err := GetShardingRule(tableName)
	if err != nil {
		return TableRule{}, nil, err
	}

	if rule.ActualDataNodes == "" {
		return rule, nil, nil
	}

	nodes, err := ParseActualDataNodes(rule.ActualDataNodes)
	if err != nil {
		return rule, nil, err
	}

	return rule, nodes, nil
}

// CalculateShardingValue calculates the sharding value based on INLINE algorithm
func CalculateShardingValue(expression string, shardingValue interface{}) (string, error) {
	modPattern := regexp.MustCompile(`\$\{(\w+)\s*%\s*(\d+)\}`)
	match := modPattern.FindStringSubmatch(expression)
	if match == nil {
		return expression, nil
	}

	modulo, _ := strconv.Atoi(match[2])

	var value interface{}
	switch v := shardingValue.(type) {
	case int:
		value = v
	case int64:
		value = int(v)
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			value = i
		} else {
			value = hash(v)
		}
	default:
		value = hash(fmt.Sprintf("%v", v))
	}

	var intValue int
	switch v := value.(type) {
	case int:
		intValue = v
	case int64:
		intValue = int(v)
	default:
		intValue = v.(int)
	}

	result := intValue % modulo

	re := regexp.MustCompile(`\$\{\w+\s*%\s*\d+\}`)
	return re.ReplaceAllString(expression, strconv.Itoa(result)), nil
}

// hash implements a string hash function for sharding
func hash(s string) int {
	h := 0
	for _, c := range s {
		h = 31*h + int(c)
	}
	return h
}

// IsSystemQuery checks if the SQL is a system query that doesn't need sharding
func IsSystemQuery(sql string) bool {
	sql = strings.TrimSpace(strings.ToUpper(sql))

	systemPatterns := []string{
		"SHOW VARIABLES",
		"SHOW DATABASES",
		"SHOW COLUMNS",
		"SHOW CREATE TABLE",
		"SHOW INDEX",
		"SHOW STATUS",
		"SELECT COUNT(*) FROM information_schema",
		"SELECT @@",
		"SET ",
		"SELECT 1",
		"SELECT DATABASE()",
		"SHOW MASTER STATUS",
		"SHOW SLAVE STATUS",
	}

	for _, pattern := range systemPatterns {
		if strings.HasPrefix(sql, pattern) {
			return true
		}
	}

	return false
}
