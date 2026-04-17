"""
Deprecated compatibility wrapper for the retired REST/WebSocket ingestion path.

The active ingestion path is `pieces_bridge.ingest_p2.run_phase2_ingest`, which
re-classifies cached summaries and fetches deltas through the Pieces MCP SSE API.
"""
from __future__ import annotations

import warnings

from pieces_bridge.ingest_p2 import run_phase2_ingest


class PiecesBridge:
    """Compatibility shim that routes old callers to the Phase 2 ingest path."""

    def __init__(self):
        warnings.warn(
            "pieces_bridge.ingest is deprecated; use pieces_bridge.ingest_p2.run_phase2_ingest().",
            DeprecationWarning,
            stacklevel=2,
        )

    def run_initial_sync(self):
        print("[deprecated] Redirecting to pieces_bridge.ingest_p2.run_phase2_ingest()")
        run_phase2_ingest()

    def start_websocket_listener(self):
        raise RuntimeError(
            "Real-time WebSocket ingestion is not supported in the compatibility wrapper. "
            "Use pieces_bridge.ingest_p2.run_phase2_ingest() for batch sync."
        )

    def start(self, websocket: bool = True):
        self.run_initial_sync()
        if websocket:
            print("[deprecated] Live streaming is no longer supported by pieces_bridge.ingest.")
        return self

    def stop(self):
        return None


if __name__ == "__main__":
    print("[deprecated] pieces_bridge.ingest now delegates to the Phase 2 ingest path.")
    run_phase2_ingest()
