import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    # IMPORTANT: log to stderr so MCP stdio (stdout) stays clean JSON-RPC only.
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
        stream=sys.stderr,
    )
