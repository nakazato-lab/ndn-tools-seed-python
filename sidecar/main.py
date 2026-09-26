import sys
import logging

logging.basicConfig(level=logging.INFO)
from ndn_function_grpc import NDNFunction

def data_request_handler(name: str) -> str:
    print(f"Interest: {name}")
    return "DATA"

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python main.py <prefix>")
        sys.exit(1)
        
    print("Starting NDN gRPC Server...")
    prefix = sys.argv[1]
    ndn = NDNFunction()
    ndn.run(prefix, data_request_handler)
