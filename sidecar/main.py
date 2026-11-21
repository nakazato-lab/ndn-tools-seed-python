"""Start the Function Sidecar."""
import logging
import sys

from server import SidecarServer
from tracing import setup_tracing


def main():
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 2:
        print("Usage: python main.py <prefix>")
        return 1
    print("Starting NDN gRPC Server...")
    provider = setup_tracing('sidecar')
    try:
        sidecar = SidecarServer()
        sidecar.run(sys.argv[1])
    finally:
        if provider is not None:
            provider.shutdown()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
