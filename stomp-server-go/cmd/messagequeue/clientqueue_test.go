package main

import (
	"fmt"
	"path/filepath"
	"testing"
)

// testClientQueueContract runs the same behavioral assertions against any
// clientQueue implementation, so both memoryClientQueue and
// sqliteClientQueue are held to the same contract.
func testClientQueueContract(t *testing.T, newQueue func() clientQueue) {
	t.Helper()

	t.Run("empty queue peeks and pops as empty", func(t *testing.T) {
		q := newQueue()
		if _, ok, err := q.PeekFront(); err != nil || ok {
			t.Fatalf("PeekFront() = (_, %v, %v), want (_, false, nil)", ok, err)
		}
		if err := q.PopFront(); err != nil {
			t.Fatalf("PopFront() on empty queue: %v", err)
		}
		if n, err := q.Len(); err != nil || n != 0 {
			t.Fatalf("Len() = (%d, %v), want (0, nil)", n, err)
		}
	})

	t.Run("enqueue preserves FIFO order", func(t *testing.T) {
		q := newQueue()
		for _, e := range []string{"a", "b", "c"} {
			if _, err := q.Enqueue(e, 100); err != nil {
				t.Fatalf("Enqueue(%q): %v", e, err)
			}
		}
		for _, want := range []string{"a", "b", "c"} {
			got, ok, err := q.PeekFront()
			if err != nil || !ok {
				t.Fatalf("PeekFront() = (%q, %v, %v)", got, ok, err)
			}
			if got != want {
				t.Fatalf("PeekFront() = %q, want %q", got, want)
			}
			if err := q.PopFront(); err != nil {
				t.Fatalf("PopFront(): %v", err)
			}
		}
		if n, _ := q.Len(); n != 0 {
			t.Fatalf("Len() = %d, want 0 after draining", n)
		}
	})

	t.Run("enqueue trims to cap and reports overflow", func(t *testing.T) {
		q := newQueue()
		var lastOverflowed bool
		for _, e := range []string{"1", "2", "3", "4"} {
			var err error
			lastOverflowed, err = q.Enqueue(e, 3)
			if err != nil {
				t.Fatalf("Enqueue(%q): %v", e, err)
			}
		}
		if !lastOverflowed {
			t.Fatal("Enqueue() overflowed = false on the 4th insert into a cap-3 queue, want true")
		}
		if n, _ := q.Len(); n != 3 {
			t.Fatalf("Len() = %d, want 3", n)
		}
		for _, want := range []string{"2", "3", "4"} {
			got, ok, _ := q.PeekFront()
			if !ok || got != want {
				t.Fatalf("PeekFront() = (%q, %v), want (%q, true)", got, ok, want)
			}
			q.PopFront()
		}
	})

	t.Run("enqueue within cap does not overflow", func(t *testing.T) {
		q := newQueue()
		overflowed, err := q.Enqueue("only", 5)
		if err != nil {
			t.Fatalf("Enqueue(): %v", err)
		}
		if overflowed {
			t.Fatal("Enqueue() overflowed = true, want false")
		}
	})

	t.Run("delete removes every retained event", func(t *testing.T) {
		q := newQueue()
		for _, e := range []string{"a", "b", "c"} {
			if _, err := q.Enqueue(e, 100); err != nil {
				t.Fatalf("Enqueue(%q): %v", e, err)
			}
		}
		if err := q.Delete(); err != nil {
			t.Fatalf("Delete(): %v", err)
		}
		if n, err := q.Len(); err != nil || n != 0 {
			t.Fatalf("Len() after Delete() = (%d, %v), want (0, nil)", n, err)
		}
		if _, ok, err := q.PeekFront(); err != nil || ok {
			t.Fatalf("PeekFront() after Delete() = (_, %v, %v), want (_, false, nil)", ok, err)
		}
	})
}

func TestMemoryClientQueue_Contract(t *testing.T) {
	testClientQueueContract(t, func() clientQueue { return newMemoryClientQueue("test", "") })
}

func TestMemoryClientQueue_StoresSubscriptionParams(t *testing.T) {
	q := newMemoryClientQueue("alice", "topic=weather&qos=1").(*memoryClientQueue)
	if q.params != "topic=weather&qos=1" {
		t.Errorf("params = %q, want topic=weather&qos=1", q.params)
	}
}

func TestSQLiteClientQueue_StoresSubscriptionParams(t *testing.T) {
	db, err := openQueueDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	q := sqliteQueueFactory(db, discardLogger())("alice", "topic=weather&qos=1").(*sqliteClientQueue)
	if q.params != "topic=weather&qos=1" {
		t.Errorf("params = %q, want topic=weather&qos=1", q.params)
	}
}

func TestSQLiteClientQueue_Contract(t *testing.T) {
	db, err := openQueueDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := sqliteQueueFactory(db, discardLogger())

	n := 0
	testClientQueueContract(t, func() clientQueue {
		n++
		// A distinct client id per subtest, since all subtests share one
		// underlying database/table.
		return factory(fmt.Sprintf("client-%d", n), "")
	})
}

func TestSQLiteClientQueue_IsolatesByClientID(t *testing.T) {
	db, err := openQueueDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := sqliteQueueFactory(db, discardLogger())

	alice, bob := factory("alice", ""), factory("bob", "")
	if _, err := alice.Enqueue("a1", 100); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := bob.Enqueue("b1", 100); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got, ok, _ := alice.PeekFront(); !ok || got != "a1" {
		t.Errorf("alice.PeekFront() = (%q, %v), want (\"a1\", true)", got, ok)
	}
	if got, ok, _ := bob.PeekFront(); !ok || got != "b1" {
		t.Errorf("bob.PeekFront() = (%q, %v), want (\"b1\", true)", got, ok)
	}
}

func TestSQLiteClientQueue_DeleteOnlyAffectsOwnClientID(t *testing.T) {
	db, err := openQueueDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := sqliteQueueFactory(db, discardLogger())

	alice, bob := factory("alice", ""), factory("bob", "")
	if _, err := alice.Enqueue("a1", 100); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := bob.Enqueue("b1", 100); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := alice.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if n, _ := alice.Len(); n != 0 {
		t.Errorf("alice.Len() after Delete() = %d, want 0", n)
	}
	if got, ok, _ := bob.PeekFront(); !ok || got != "b1" {
		t.Errorf("bob.PeekFront() after alice.Delete() = (%q, %v), want (\"b1\", true) -- unaffected", got, ok)
	}
}

func TestSQLiteClientQueue_PersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.sqlite3")

	db1, err := openQueueDB(path)
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	q1 := sqliteQueueFactory(db1, discardLogger())("alice", "")
	if _, err := q1.Enqueue("Event 1", 100); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q1.Enqueue("Event 2", 100); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("close db1: %v", err)
	}

	// Simulate a process restart: reopen the same file with a fresh *sql.DB.
	db2, err := openQueueDB(path)
	if err != nil {
		t.Fatalf("reopen openQueueDB: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	q2 := sqliteQueueFactory(db2, discardLogger())("alice", "")

	got, ok, err := q2.PeekFront()
	if err != nil || !ok {
		t.Fatalf("PeekFront() after reopen = (%q, %v, %v), want a value", got, ok, err)
	}
	if got != "Event 1" {
		t.Fatalf("PeekFront() after reopen = %q, want %q", got, "Event 1")
	}
	if n, _ := q2.Len(); n != 2 {
		t.Fatalf("Len() after reopen = %d, want 2", n)
	}
}

func TestSQLiteQueueFactory_PersistsClientMetadata(t *testing.T) {
	db, err := openQueueDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	sqliteQueueFactory(db, discardLogger())("alice", "osdf/vdc/public/pelican_protocol")

	clients, err := listPersistedClients(db)
	if err != nil {
		t.Fatalf("listPersistedClients: %v", err)
	}
	if len(clients) != 1 || clients[0].clientID != "alice" || clients[0].params != "osdf/vdc/public/pelican_protocol" {
		t.Errorf("listPersistedClients() = %+v, want exactly one alice entry with the given params", clients)
	}
}

func TestSQLiteQueueFactory_UpsertsMetadataOnReuse(t *testing.T) {
	db, err := openQueueDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := sqliteQueueFactory(db, discardLogger())

	factory("alice", "osdf/vdc/public/a")
	factory("alice", "osdf/vdc/public/b")

	clients, err := listPersistedClients(db)
	if err != nil {
		t.Fatalf("listPersistedClients: %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("listPersistedClients() = %+v, want exactly one row (updated, not duplicated)", clients)
	}
	if clients[0].params != "osdf/vdc/public/b" {
		t.Errorf("params = %q, want the most recent value osdf/vdc/public/b", clients[0].params)
	}
}

func TestSQLiteClientQueue_DeleteRemovesMetadata(t *testing.T) {
	db, err := openQueueDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := sqliteQueueFactory(db, discardLogger())

	alice := factory("alice", "osdf/vdc/public/a")
	factory("bob", "osdf/vdc/public/b")

	if err := alice.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	clients, err := listPersistedClients(db)
	if err != nil {
		t.Fatalf("listPersistedClients: %v", err)
	}
	if len(clients) != 1 || clients[0].clientID != "bob" {
		t.Errorf("listPersistedClients() after alice.Delete() = %+v, want only bob remaining", clients)
	}
}

func TestListPersistedClients_EmptyWhenNoneYet(t *testing.T) {
	db, err := openQueueDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("openQueueDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	clients, err := listPersistedClients(db)
	if err != nil {
		t.Fatalf("listPersistedClients: %v", err)
	}
	if len(clients) != 0 {
		t.Errorf("listPersistedClients() on a fresh database = %+v, want empty", clients)
	}
}
