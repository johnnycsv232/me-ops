"""Compatibility shim for the ME-OPS UI server."""
from ui.server import run_server


def serve(host: str = "127.0.0.1", port: int = 8008) -> None:
    run_server(host=host, port=port)


if __name__ == "__main__":
    serve()
