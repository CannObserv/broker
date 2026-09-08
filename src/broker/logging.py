"""Structured JSON logging utilities.

`build_json_formatter()` is the single source of truth for the JSON field
contract (`timestamp`, `level`, `logger`, `message`).
`configure_logging()` wires it onto the root logger.

Service-local by policy, not a mirror. Archiver and Watcher each keep their own
copy of this module and there is no parity requirement between them
(CannObserv/watcher#159, #236); the JSON *field contract* is what the cluster
shares, not the file. Divergence here is allowed and expected.
"""

import logging
import sys

from pythonjsonlogger.json import JsonFormatter


def build_json_formatter() -> JsonFormatter:
    """Build the canonical JSON formatter.

    Emits ``timestamp`` (ISO-8601 UTC), ``level``, ``logger``, and ``message``.
    Without an explicit fmt string, python-json-logger derives keys from the
    ``%(field)s`` placeholders, which default to ``"%(message)s"`` alone -
    dropping level, logger name, and timestamp (archiver#115). This factory is
    the one place that string lives, so the app logger and the uvicorn
    log-config cannot drift (archiver#122).
    """
    return JsonFormatter(
        "%(levelname)s %(name)s %(message)s",
        timestamp=True,
        rename_fields={"levelname": "level", "name": "logger"},
    )


class ColorMessageFilter(logging.Filter):
    """Drop uvicorn's `color_message` extra before anything serializes it.

    uvicorn attaches an ANSI-coloured duplicate of each lifecycle message as
    ``extra={"color_message": ...}`` for its own colour-aware formatter. Under
    our JSON formatter that extra leaks into the payload with raw escapes, so
    strip it at the record source - once, before any handler reads the record -
    rather than in a single sink (archiver#123).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Strip the extra if present. Never drops a record."""
        record.__dict__.pop("color_message", None)
        return True


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logger with JSON formatting. Call once at entry points."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(build_json_formatter())
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Use in modules as: logger = get_logger(__name__)"""
    return logging.getLogger(name)
