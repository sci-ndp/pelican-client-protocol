from __future__ import annotations

from dataclasses import dataclass, field


class ProtocolError(Exception):
    """Raised when bytes do not form a valid STOMP 1.2 frame."""


@dataclass
class Frame:
    command: str
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""

    def encode(self) -> bytes:
        # STOMP 1.2 uses a blank line to separate headers from the body and a NULL terminator.
        lines = [self.command]
        lines.extend(f"{key}:{escape(value)}" for key, value in self.headers.items())
        return ("\n".join(lines) + "\n\n" + self.body + "\x00").encode()


def escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace(":", "\\c").replace("\r", "\\r")


def unescape(value: str) -> str:
    out, i = [], 0
    replacements = {"n": "\n", "r": "\r", "c": ":", "\\": "\\"}
    while i < len(value):
        if value[i] == "\\" and i + 1 < len(value):
            if value[i + 1] not in replacements:
                raise ProtocolError(f"invalid header escape: {value[i:i + 2]}")
            out.append(replacements[value[i + 1]]); i += 2
        else:
            out.append(value[i]); i += 1
    return "".join(out)


def parse(raw: bytes) -> Frame:
    if not raw.endswith(b"\x00"):
        raise ProtocolError("frame is missing NULL terminator")
    text = raw[:-1].decode("utf-8")
    header_text, separator, body = text.partition("\n\n")
    if not separator:
        raise ProtocolError("frame is missing header/body separator")
    lines = header_text.split("\n")
    if not lines or not lines[0]:
        raise ProtocolError("frame has no command")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line: continue
        if ":" not in line: raise ProtocolError("header is missing ':'")
        key, value = line.split(":", 1)
        if key in headers: raise ProtocolError(f"duplicate header: {key}")
        headers[unescape(key)] = unescape(value)
    return Frame(lines[0], headers, body)


async def read_frame(websocket) -> Frame:
    data = await websocket.recv()
    if isinstance(data, str): data = data.encode()
    # A STOMP heartbeat is a lone LF and is not a frame with a NULL terminator.
    if data in (b"\n", b"\r\n"):
        return Frame("HEARTBEAT")
    return parse(data)
