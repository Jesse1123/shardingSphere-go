package sqlrouter

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// Transaction represents a distributed transaction
type Transaction struct {
	ID        string
	xaEnabled bool
}

// TransactionManager manages database transactions
type TransactionManager struct {
	mu           sync.RWMutex
	transactions map[string]*TransactionContext
	timeout      time.Duration
}

// TransactionContext holds transaction context for a connection
type TransactionContext struct {
	ID          string
	Tx          *sql.Tx
	DataSources map[string]*sql.Tx
	Status      TransactionStatus
	StartTime   time.Time
}

// TransactionStatus represents transaction status
type TransactionStatus int

const (
	TransactionActive TransactionStatus = iota
	TransactionPrepared
	TransactionCommitted
	TransactionRolledBack
)

var globalTransactionManager *TransactionManager
var txManagerOnce sync.Once

// GetTransactionManager returns the global transaction manager instance
func GetTransactionManager() *TransactionManager {
	txManagerOnce.Do(func() {
		globalTransactionManager = &TransactionManager{
			transactions: make(map[string]*TransactionContext),
			timeout:      30 * time.Minute,
		}
	})
	return globalTransactionManager
}

// BeginTransaction begins a new transaction on a specific data source
func (tm *TransactionManager) BeginTransaction(dataSource string) (*TransactionContext, error) {
	db, err := getDBConnection(dataSource)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	ctx := &TransactionContext{
		ID:        generateTransactionID(),
		Tx:        tx,
		DataSources: map[string]*sql.Tx{dataSource: tx},
		Status:    TransactionActive,
		StartTime: time.Now(),
	}

	tm.mu.Lock()
	tm.transactions[ctx.ID] = ctx
	tm.mu.Unlock()

	return ctx, nil
}

// JoinTransaction joins an existing transaction with a new data source
func (tm *TransactionManager) JoinTransaction(txID, dataSource string) error {
	tm.mu.RLock()
	ctx, exists := tm.transactions[txID]
	tm.mu.RUnlock()

	if !exists {
		return ErrTransactionNotFound
	}

	if ctx.Status != TransactionActive {
		return ErrTransactionNotActive
	}

	if _, exists := ctx.DataSources[dataSource]; exists {
		return nil // Already joined
	}

	db, err := getDBConnection(dataSource)
	if err != nil {
		return err
	}

	// Create a new transaction on this data source
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	ctx.DataSources[dataSource] = tx
	return nil
}

// CommitTransaction commits a distributed transaction
func (tm *TransactionManager) CommitTransaction(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	ctx, exists := tm.transactions[txID]
	if !exists {
		return ErrTransactionNotFound
	}

	if ctx.Status != TransactionActive {
		return ErrTransactionNotActive
	}

	// Commit all data source transactions
	for _, tx := range ctx.DataSources {
		if err := tx.Commit(); err != nil {
			// Rollback others on failure
			for _, rbTx := range ctx.DataSources {
				_ = rbTx.Rollback()
			}
			return err
		}
	}

	ctx.Status = TransactionCommitted
	delete(tm.transactions, txID)
	return nil
}

// RollbackTransaction rolls back a distributed transaction
func (tm *TransactionManager) RollbackTransaction(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	ctx, exists := tm.transactions[txID]
	if !exists {
		return ErrTransactionNotFound
	}

	// Rollback all data source transactions
	for _, tx := range ctx.DataSources {
		_ = tx.Rollback()
	}

	ctx.Status = TransactionRolledBack
	delete(tm.transactions, txID)
	return nil
}

// GetTransaction retrieves a transaction by ID
func (tm *TransactionManager) GetTransaction(txID string) (*TransactionContext, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	ctx, exists := tm.transactions[txID]
	if !exists {
		return nil, ErrTransactionNotFound
	}
	return ctx, nil
}

// CloseTransaction closes and cleans up a transaction
func (tm *TransactionManager) CloseTransaction(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if ctx, exists := tm.transactions[txID]; exists {
		if ctx.Status == TransactionActive {
			_ = tm.rollbackInternal(ctx)
		}
		delete(tm.transactions, txID)
	}
	return nil
}

func (tm *TransactionManager) rollbackInternal(ctx *TransactionContext) error {
	for _, tx := range ctx.DataSources {
		_ = tx.Rollback()
	}
	return nil
}

// ErrTransactionNotFound is returned when a transaction is not found
var ErrTransactionNotFound = &TransactionError{Message: "transaction not found"}

// ErrTransactionNotActive is returned when a transaction is not active
var ErrTransactionNotActive = &TransactionError{Message: "transaction is not active"}

// TransactionError represents a transaction error
type TransactionError struct {
	Message string
}

func (e *TransactionError) Error() string {
	return e.Message
}

// generateTransactionID generates a unique transaction ID
func generateTransactionID() string {
	return time.Now().Format("20060102150405") + "-" + randomString(8)
}

// randomString generates a random string of given length
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}

// ExecuteInTransaction executes a function within a transaction context
func ExecuteInTransaction(ctx context.Context, dataSource string, fn func(*sql.Tx) error) error {
	db, err := getDBConnection(dataSource)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return rbErr
		}
		return err
	}

	return tx.Commit()
}

// ConnectionPoolStats holds connection pool statistics
type ConnectionPoolStats struct {
	DataSource    string
	ActiveConns   int
	IdleConns     int
	TotalConns    int
	MaxOpenConns  int
	WaitQueueLen  int
}

// GetPoolStats returns statistics for all connection pools
func GetPoolStats() []ConnectionPoolStats {
	dbMutex.RLock()
	defer dbMutex.RUnlock()

	stats := make([]ConnectionPoolStats, 0, len(dbPools))
	for name, db := range dbPools {
		s := ConnectionPoolStats{
			DataSource:   name,
			MaxOpenConns: db.Stats().MaxOpenConnections,
		}
		if db.Stats().OpenConnections > 0 {
			s.TotalConns = db.Stats().OpenConnections
			s.ActiveConns = db.Stats().InUse
			s.IdleConns = db.Stats().Idle
		}
		stats = append(stats, s)
	}
	return stats
}

// CloseAllConnections closes all database connections
func CloseAllConnections() {
	dbMutex.Lock()
	defer dbMutex.Unlock()

	for name, db := range dbPools {
		if err := db.Close(); err != nil {
			continue
		}
		delete(dbPools, name)
	}
}
