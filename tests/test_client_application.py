import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "stomp-client"))

from persistence import ClientPersistence


def test_application_inbox_is_idempotent_and_durable(tmp_path):
    path = tmp_path / "client.sqlite3"
    state = ClientPersistence(str(path), "client-a")
    assert state.record_event("event-1", "/origin/demo", '{"value": 1}')
    assert not state.record_event("event-1", "/origin/demo", '{"value": 1}')
    assert state.processed_count == 1
    state.close()
    reopened = ClientPersistence(str(path), "client-a")
    assert not reopened.record_event("event-1", "/origin/demo", '{"value": 1}')
    assert reopened.record_event("event-2", "/origin/demo", '{"value": 2}')
    assert reopened.processed_count == 2
    reopened.close()
