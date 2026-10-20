"""A local HTTP server standing in for GitHub's REST API and for co-status (#110).

Ported from CannObserv/processor's ``tests/conftest.py`` (``HttpStub``, at
``540975e``). The drift check's calls go through ``urllib`` against it, so a
refusal, an error page and a timeout are the real client's, never a fake's. No
test reaches either service: the fixtures are in ``tests/conftest.py``.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class HttpStub:
    """Answers from ``routes``; records every request it gets."""

    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], tuple[int, bytes, float]] = {}
        self.requests: list[dict] = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def _answer(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                stub.requests.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        # The message itself: case-insensitive, as HTTP's names are.
                        # urllib sends `X-api-key`, and co-status reads it.
                        "headers": self.headers,
                        "body": self.rfile.read(length) if length else b"",
                    }
                )
                code, body, delay = stub.routes.get(
                    (self.command, self.path), (404, b'{"message": "unrouted"}', 0.0)
                )
                time.sleep(delay)
                try:
                    self.send_response(code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    # The client gave up first: what the timeout tests provoke. Left to
                    # socketserver, its traceback reaches stderr in a later test.
                    pass

            do_GET = do_POST = _answer

            def log_message(self, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        # Joined at close, so no answer still sleeping outlives its test.
        self.server.daemon_threads = False
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
        ).start()

    def route(
        self, method: str, path: str, code: int = 200, body: object = None, delay: float = 0.0
    ) -> None:
        """Answer ``method path`` (query string included) with ``code`` and ``body``."""
        raw = body if isinstance(body, bytes) else json.dumps({} if body is None else body).encode()
        self.routes[(method, path)] = (code, raw, delay)

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
