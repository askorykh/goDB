package engine

import (
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
)

// DBEngine is the main database engine struct.
// Later it will have references to storage, catalog, transaction manager, etc.
type DBEngine struct {
	started bool
	store   storage.Engine
	inTx    bool
	currTx  storage.Tx
}

// New creates a new DBEngine instance.
// For now, it just returns an empty engine.
func New(store storage.Engine) *DBEngine {
	return &DBEngine{
		started: false,
		store:   store,
		inTx:    false,
	}
}

// Start runs initialization steps for the engine.
// Later this will open storage, load metadata, etc.
func (e *DBEngine) Start() error {
	if e.started {
		return fmt.Errorf("engine already started")
	}
	e.started = true
	return nil
}

// ListTables returns the names of all tables in the storage engine.
func (e *DBEngine) ListTables() ([]string, error) {
	if !e.started {
		return nil, fmt.Errorf("engine not started")
	}

	return e.store.ListTables()
}

// TableSchema returns the column definitions for a table.
func (e *DBEngine) TableSchema(name string) ([]sql.Column, error) {
	if !e.started {
		return nil, fmt.Errorf("engine not started")
	}

	return e.store.TableSchema(name)
}
