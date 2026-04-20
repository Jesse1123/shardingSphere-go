package sqlrouter

import (
	"fmt"
	"regexp"
	"shardingSphere-go/config"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ShardingStrategy defines interface for database and table sharding strategies
type ShardingStrategy interface {
	Calculate(shardingValue interface{}) (string, error)
}

// InlineShardingStrategy implements INLINE sharding algorithm
type InlineShardingStrategy struct {
	Expression    string
	ShardingColumn string
}

// NewInlineShardingStrategy creates a new INLINE sharding strategy
func NewInlineShardingStrategy(expression, shardingColumn string) *InlineShardingStrategy {
	return &InlineShardingStrategy{
		Expression:    expression,
		ShardingColumn: shardingColumn,
	}
}

// Calculate calculates the sharding result based on the value
func (s *InlineShardingStrategy) Calculate(shardingValue interface{}) (string, error) {
	expression := s.Expression
	
	// Parse and replace ${column % mod} pattern
	modPattern := regexp.MustCompile(`\$\{(\w+)\s*%\s*(\d+)\}`)
	match := modPattern.FindStringSubmatch(expression)
	if match != nil {
		_, _ = match[1], match[2] // We already have sharding column from config
		modulo, _ := strconv.Atoi(match[2])

		var intValue int
		switch v := shardingValue.(type) {
		case int:
			intValue = v
		case int64:
			intValue = int(v)
		case string:
			// Try to parse as int first
			if i, err := strconv.Atoi(v); err == nil {
				intValue = i
			} else {
				// Use hash for string values
				intValue = hashString(v)
			}
		default:
			intValue = hashString(fmt.Sprintf("%v", v))
		}

		result := intValue % modulo
		expression = modPattern.ReplaceAllString(expression, strconv.Itoa(result))
	}

	// Parse and replace ${column} pattern without modulo
	simplePattern := regexp.MustCompile(`\$\{(\w+)\}`)
	expression = simplePattern.ReplaceAllStringFunc(expression, func(match string) string {
		return fmt.Sprintf("%v", shardingValue)
	})

	return expression, nil
}

// ModShardingStrategy implements simple modulo sharding
type ModShardingStrategy struct {
	Modulo int
}

// NewModShardingStrategy creates a new modulo sharding strategy
func NewModShardingStrategy(modulo int) *ModShardingStrategy {
	return &ModShardingStrategy{Modulo: modulo}
}

// Calculate calculates the sharding result
func (s *ModShardingStrategy) Calculate(shardingValue interface{}) (string, error) {
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
	return strconv.Itoa(intValue % s.Modulo), nil
}

// hashString computes a hash of a string
func hashString(s string) int {
	h := 0
	for _, c := range s {
		h = 31*h + int(c)
	}
	return h
}

// GetShardingAlgorithm returns a sharding strategy based on algorithm name
func GetShardingAlgorithm(algorithmName string) (ShardingStrategy, error) {
	algorithm, err := config.GetShardingAlgorithm(algorithmName)
	if err != nil {
		return nil, err
	}

	switch strings.ToUpper(algorithm.Type) {
	case "INLINE":
		expression := algorithm.Props["algorithm-expression"]
		return NewInlineShardingStrategy(expression, ""), nil
	case "MOD":
		moduloStr := algorithm.Props["mod"]
		modulo, _ := strconv.Atoi(moduloStr)
		return NewModShardingStrategy(modulo), nil
	default:
		// Default to INLINE
		expression := algorithm.Props["algorithm-expression"]
		return NewInlineShardingStrategy(expression, ""), nil
	}
}

// RouteResult represents a routing result for a SQL
type RouteResult struct {
	SQL        string
	DataSource string
	Table      string
}

// RouteDataNodes represents a list of route results
type RouteDataNodes []RouteResult

// SnowflakeKeyGenerator generates snowflake IDs
type SnowflakeKeyGenerator struct {
	workerID     int64
	mu           sync.Mutex
	lastTime     int64
	sequence     int64
	epoch        int64
	workerIDBits uint8
	seqBits      uint8
}

// NewSnowflakeKeyGenerator creates a new snowflake key generator
func NewSnowflakeKeyGenerator(workerID int64) *SnowflakeKeyGenerator {
	return &SnowflakeKeyGenerator{
		workerID:     workerID,
		epoch:        1609459200000, // 2021-01-01
		workerIDBits: 10,
		seqBits:      12,
	}
}

// Generate generates a snowflake ID
func (g *SnowflakeKeyGenerator) Generate() int64 {
	const (
		maxWorkerID = (1 << 10) - 1
		maxSeq      = (1 << 12) - 1
	)

	g.mu.Lock()
	defer g.mu.Unlock()

	curTime := time.Now().UnixNano() / 1e6 // Convert to milliseconds
	if curTime < g.lastTime {
		curTime = g.lastTime
	}
	if curTime == g.lastTime {
		g.sequence = (g.sequence + 1) & maxSeq
		if g.sequence == 0 {
			for curTime <= g.lastTime {
				curTime = time.Now().UnixNano() / 1e6
			}
		}
	} else {
		g.sequence = 0
	}
	g.lastTime = curTime

	id := ((curTime - g.epoch) << (10 + 12)) |
		((g.workerID & int64(maxWorkerID)) << 12) |
		g.sequence

	return id
}

// GenerateKey generates a key using the configured generator
func GenerateKey(generatorName string) (int64, error) {
	generator, err := config.GetKeyGenerator(generatorName)
	if err != nil {
		return 0, err
	}

	switch strings.ToUpper(generator.Type) {
	case "SNOWFLAKE":
		workerIDStr := generator.Props["worker-id"]
		workerID := int64(0)
		if workerIDStr != "" {
			fmt.Sscanf(workerIDStr, "%d", &workerID)
		}
		gen := NewSnowflakeKeyGenerator(workerID)
		return gen.Generate(), nil
	default:
		return 0, fmt.Errorf("unsupported key generator type: %s", generator.Type)
	}
}
