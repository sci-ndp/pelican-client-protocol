from __future__ import annotations

import logging
import sys

RESET = "\033[0m"
COLORS = {"CLIENT -> SERVER": "\033[36m", "SERVER -> CLIENT": "\033[32m", "SYSTEM": "\033[33m"}


def configure_logging() -> logging.Logger:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    return logging.getLogger("stomp-server")


def frame_log(logger: logging.Logger, direction: str, command: str, detail: str = "") -> None:
    label = f"{COLORS.get(direction, '')}{direction}{RESET}"
    logger.info("%s  %s%s", label, command, f" {detail}" if detail else "")
