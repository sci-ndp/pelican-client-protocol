package clientqueue

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// testQueueContract runs the same behavioral assertions against any Queue
// implementation, so both MemoryQueue and the SQLite one are held to the
// same contract.
func testQueueContract(t *testing.T, newQueue func() Queue) {
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

func TestMemoryQueue_Contract(t *testing.T) {
	testQueueContract(t, func() Queue { return NewMemoryQueue("test", "") })
}

func TestMemoryQueue_StoresSubscriptionParams(t *testing.T) {
	q := NewMemoryQueue("alice", "topic=weather&qos=1").(*MemoryQueue)
	if q.params != "topic=weather&qos=1" {
		t.Errorf("params = %q, want topic=weather&qos=1", q.params)
	}
}

func TestSQLiteQueue_StoresSubscriptionParams(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	q := SQLiteFactory(db, discardLogger())("alice", "topic=weather&qos=1").(*sqliteQueue)
	if q.params != "topic=weather&qos=1" {
		t.Errorf("params = %q, want topic=weather&qos=1", q.params)
	}
}

func TestSQLiteQueue_Contract(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := SQLiteFactory(db, discardLogger())

	n := 0
	testQueueContract(t, func() Queue {
		n++
		// A distinct client id per subtest, since all subtests share one
		// underlying database/table.
		return factory(fmt.Sprintf("client-%d", n), "")
	})
}

func TestSQLiteQueue_IsolatesByClientID(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := SQLiteFactory(db, discardLogger())

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

func TestSQLiteQueue_DeleteOnlyAffectsOwnClientID(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := SQLiteFactory(db, discardLogger())

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

func TestSQLiteQueue_PersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.sqlite3")

	db1, err := OpenDB(path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	q1 := SQLiteFactory(db1, discardLogger())("alice", "")
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
	db2, err := OpenDB(path)
	if err != nil {
		t.Fatalf("reopen OpenDB: %v", err)
	}
	t.Cleanup(func() { db2.Close() })
	q2 := SQLiteFactory(db2, discardLogger())("alice", "")

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

func TestSQLiteFactory_PersistsClientMetadata(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	SQLiteFactory(db, discardLogger())("alice", "osdf/vdc/public/pelican_protocol")

	clients, err := ListPersistedClients(db)
	if err != nil {
		t.Fatalf("ListPersistedClients: %v", err)
	}
	if len(clients) != 1 || clients[0].ClientID != "alice" || clients[0].Params != "osdf/vdc/public/pelican_protocol" {
		t.Errorf("ListPersistedClients() = %+v, want exactly one alice entry with the given params", clients)
	}
}

func TestSQLiteFactory_UpsertsMetadataOnReuse(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := SQLiteFactory(db, discardLogger())

	factory("alice", "osdf/vdc/public/a")
	factory("alice", "osdf/vdc/public/b")

	clients, err := ListPersistedClients(db)
	if err != nil {
		t.Fatalf("ListPersistedClients: %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("ListPersistedClients() = %+v, want exactly one row (updated, not duplicated)", clients)
	}
	if clients[0].Params != "osdf/vdc/public/b" {
		t.Errorf("params = %q, want the most recent value osdf/vdc/public/b", clients[0].Params)
	}
}

func TestSQLiteQueue_DeleteRemovesMetadata(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	factory := SQLiteFactory(db, discardLogger())

	alice := factory("alice", "osdf/vdc/public/a")
	factory("bob", "osdf/vdc/public/b")

	if err := alice.Delete(); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	clients, err := ListPersistedClients(db)
	if err != nil {
		t.Fatalf("ListPersistedClients: %v", err)
	}
	if len(clients) != 1 || clients[0].ClientID != "bob" {
		t.Errorf("ListPersistedClients() after alice.Delete() = %+v, want only bob remaining", clients)
	}
}

func TestListPersistedClients_EmptyWhenNoneYet(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	clients, err := ListPersistedClients(db)
	if err != nil {
		t.Fatalf("ListPersistedClients: %v", err)
	}
	if len(clients) != 0 {
		t.Errorf("ListPersistedClients() on a fresh database = %+v, want empty", clients)
	}
}
