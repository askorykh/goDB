package engine

import "fmt"

func (e *DBEngine) beginTx() error {
	if !e.started {
		return fmt.Errorf("engine not started")
	}
	if e.inTx {
		return fmt.Errorf("transaction already in progress")
	}

	tx, err := e.store.Begin(false) // writeable transaction
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	e.currTx = tx
	e.inTx = true
	return nil
}

func (e *DBEngine) commitTx() error {
	if !e.inTx {
		return fmt.Errorf("no active transaction to commit")
	}

	if err := e.store.Commit(e.currTx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	e.currTx = nil
	e.inTx = false
	return nil
}

func (e *DBEngine) rollbackTx() error {
	if !e.inTx {
		return fmt.Errorf("no active transaction to rollback")
	}

	if err := e.store.Rollback(e.currTx); err != nil {
		return fmt.Errorf("rollback tx: %w", err)
	}

	e.currTx = nil
	e.inTx = false
	return nil
}
