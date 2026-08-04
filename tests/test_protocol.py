import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "stomp-server"))

from protocol import Frame, ProtocolError, parse


def test_frame_round_trip_and_escaping():
    original = Frame("SEND", {"destination": "/a:b", "note": "line\nnext"}, "hello")
    parsed = parse(original.encode())
    assert parsed == original


def test_missing_null_is_rejected():
    try:
        parse(b"CONNECT\n\n")
    except ProtocolError:
        pass
    else:
        raise AssertionError("expected ProtocolError")


def test_duplicate_headers_are_rejected():
    try:
        parse(b"CONNECT\nhost:a\nhost:b\n\n\x00")
    except ProtocolError:
        pass
    else:
        raise AssertionError("expected ProtocolError")
