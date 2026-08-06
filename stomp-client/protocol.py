from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Frame:
    command: str
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""

    def encode(self) -> bytes:
        def esc(v: str) -> str:
            return v.replace("\\", "\\\\").replace("\n", "\\n").replace(":", "\\c").replace("\r", "\\r")
        return (self.command + "\n" + "\n".join(f"{k}:{esc(v)}" for k, v in self.headers.items()) + "\n\n" + self.body + "\x00").encode()


def parse(raw: bytes) -> Frame:
    text = raw.rstrip(b"\x00").decode()
    head, _, body = text.partition("\n\n")
    lines = head.splitlines()
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key] = value.replace("\\n", "\n").replace("\\r", "\r").replace("\\c", ":").replace("\\\\", "\\")
    return Frame(lines[0], headers, body)


def unsubscribe(subscription_id: str, receipt: str | None = None) -> Frame:
    '''Build a STOMP 1.2 UNSUBSCRIBE frame for the exact SUBSCRIBE id.

    The STOMP 1.2 id header is mandatory and identifies the subscription to
    remove. receipt is optional; callers can request it for confirmation.
    '''
    subscription_id = subscription_id.strip()
    if not subscription_id:
        raise ValueError("UNSUBSCRIBE requires a non-empty subscription id")
    headers = {"id": subscription_id}
    if receipt is not None:
        receipt = receipt.strip()
        if not receipt:
            raise ValueError("UNSUBSCRIBE receipt must be non-empty when provided")
        headers["receipt"] = receipt
    return Frame("UNSUBSCRIBE", headers)
