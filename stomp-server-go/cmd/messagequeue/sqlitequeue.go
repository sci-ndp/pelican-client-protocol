package main

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// openQueueDB opens (creating if necessary) the SQLite database at path used
// to back sqliteClientQueue, and ensures its schema exists. The returned DB
// is restricted to a single open connection: messageQueueApp already
// serializes every queue operation through its own mutex, so there's never
// more than one goroutine touching the database at a time, and forcing a
// single connection sidesteps SQLite "database is locked" errors that
// otherwise show up under Go's default connection pooling.
func openQueueDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open queue database: %w", err)
	}
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy_timeout: %w", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS queue_events (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			client_id TEXT NOT NULL,
			body      TEXT NOT NULL
		)
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create queue_events table: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_queue_events_client_id ON queue_events(client_id, id)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create queue_events index: %w", err)
	}
	return db, nil
}

// sqliteClientQueue is a clientQueue backed by a shared SQLite database,
// scoped to one client_id. Its contents survive process restarts.
type sqliteClientQueue struct {
	db       *sql.DB
	clientID string
}

// sqliteQueueFactory returns a queueFactory whose clientQueues all share db.
func sqliteQueueFactory(db *sql.DB) queueFactory {
	return func(clientID string) clientQueue {
		return &sqliteClientQueue{db: db, clientID: clientID}
	}
}

func (q *sqliteClientQueue) Enqueue(event string, cap int) (bool, error) {
	tx, err := q.db.Begin()
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`INSERT INTO queue_events (client_id, body) VALUES (?, ?)`, q.clientID, event); err != nil {
		return false, fmt.Errorf("insert: %w", err)
	}
	result, err := tx.Exec(`
		DELETE FROM queue_events
		WHERE client_id = ? AND id NOT IN (
			SELECT id FROM queue_events WHERE client_id = ? ORDER BY id DESC LIMIT ?
		)
	`, q.clientID, q.clientID, cap)
	if err != nil {
		return false, fmt.Errorf("trim: %w", err)
	}
	dropped, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("trim rows affected: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return dropped > 0, nil
}

func (q *sqliteClientQueue) PeekFront() (string, bool, error) {
	var body string
	err := q.db.QueryRow(`SELECT body FROM queue_events WHERE client_id = ? ORDER BY id ASC LIMIT 1`, q.clientID).Scan(&body)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("peek front: %w", err)
	}
	return body, true, nil
}

func (q *sqliteClientQueue) PopFront() error {
	_, err := q.db.Exec(`
		DELETE FROM queue_events WHERE id = (
			SELECT id FROM queue_events WHERE client_id = ? ORDER BY id ASC LIMIT 1
		)
	`, q.clientID)
	if err != nil {
		return fmt.Errorf("pop front: %w", err)
	}
	return nil
}

func (q *sqliteClientQueue) Len() (int, error) {
	var n int
	if err := q.db.QueryRow(`SELECT COUNT(*) FROM queue_events WHERE client_id = ?`, q.clientID).Scan(&n); err != nil {
		return 0, fmt.Errorf("len: %w", err)
	}
	return n, nil
}

func (q *sqliteClientQueue) Delete() error {
	if _, err := q.db.Exec(`DELETE FROM queue_events WHERE client_id = ?`, q.clientID); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return nil
}
