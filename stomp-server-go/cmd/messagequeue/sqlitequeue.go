package main

import (
	"database/sql"
	"fmt"
	"log/slog"

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
	// client_metadata records each client's subscription parameters
	// separately from its queued events, so a restarted process can
	// rehydrate which clients (and, for an EventSource that cares, which
	// directories/etc. their params name) existed before any of them
	// reconnect -- see messageQueueApp.RehydrateQueues and
	// listPersistedClients. Deliberately its own table rather than a column
	// on queue_events: params describe the client, not any one event, and
	// a client's queue can be legitimately empty (all events delivered and
	// acked) while it's still a client this process should remember.
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS client_metadata (
			client_id TEXT PRIMARY KEY,
			params    TEXT NOT NULL
		)
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create client_metadata table: %w", err)
	}
	return db, nil
}

// sqliteClientQueue is a clientQueue backed by a shared SQLite database,
// scoped to one client_id. Its contents survive process restarts.
type sqliteClientQueue struct {
	db       *sql.DB
	clientID string

	// params is the subscription parameters this client's queue was created
	// with (see queueFactory and Params).
	params string
}

// sqliteQueueFactory returns a queueFactory whose clientQueues all share db.
// Every call records clientID's current params in client_metadata (an
// upsert, so calling it again for a clientID that already has a row just
// refreshes its params) -- this runs for both a genuine new SUBSCRIBE and a
// startup rehydration (see messageQueueApp.RehydrateQueues), so
// client_metadata self-heals if it's ever behind what queue_events has.
func sqliteQueueFactory(db *sql.DB, log *slog.Logger) queueFactory {
	return func(clientID string, params string) clientQueue {
		if _, err := db.Exec(`
			INSERT INTO client_metadata (client_id, params) VALUES (?, ?)
			ON CONFLICT(client_id) DO UPDATE SET params = excluded.params
		`, clientID, params); err != nil {
			log.Error("failed to persist client metadata", "client_id", clientID, "error", err)
		}
		return &sqliteClientQueue{db: db, clientID: clientID, params: params}
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
	if _, err := q.db.Exec(`DELETE FROM client_metadata WHERE client_id = ?`, q.clientID); err != nil {
		return fmt.Errorf("delete metadata: %w", err)
	}
	return nil
}

func (q *sqliteClientQueue) Params() string { return q.params }

// persistedClient is one client_id/params pair recorded in client_metadata.
type persistedClient struct {
	clientID string
	params   string
}

// listPersistedClients reads every client_id/params pair client_metadata
// currently has, so a restarted process can rehydrate its in-memory client
// queues (see messageQueueApp.RehydrateQueues) for durable clients that
// haven't reconnected yet.
func listPersistedClients(db *sql.DB) ([]persistedClient, error) {
	rows, err := db.Query(`SELECT client_id, params FROM client_metadata`)
	if err != nil {
		return nil, fmt.Errorf("list persisted clients: %w", err)
	}
	defer rows.Close()

	var clients []persistedClient
	for rows.Next() {
		var c persistedClient
		if err := rows.Scan(&c.clientID, &c.params); err != nil {
			return nil, fmt.Errorf("scan persisted client: %w", err)
		}
		clients = append(clients, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list persisted clients: %w", err)
	}
	return clients, nil
}
