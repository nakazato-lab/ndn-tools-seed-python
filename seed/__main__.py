import argparse
import asyncio
import logging
import os
import signal
from urllib.parse import urlparse

from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.security import KeychainDigest
from ndn.transport.stream_face import TcpFace, UnixFace

from .backend import KubernetesBackend
from .server import SeedServer


class RemoteTcpFace(TcpFace):
    def isLocalFace(self):
        # A loopback address may be a Docker port forward to a remote NFD.
        # Match the original C++ Seed's /localhop/nfd registration on TCP.
        return False


def make_face(transport):
    uri = urlparse(transport)
    if uri.scheme == 'unix' and uri.path:
        return UnixFace(uri.path)
    if uri.scheme in ('tcp', 'tcp4', 'tcp6') and uri.hostname:
        return RemoteTcpFace(uri.hostname, uri.port or 6363)
    raise ValueError('NDN transport must be unix:///path or tcp://host:port')


def arguments():
    parser = argparse.ArgumentParser(description='Deploy Python functions from NDN CREATE/DELETE Interests')
    parser.add_argument('name', nargs='?', default=os.getenv('SEED_PREFIX'))
    parser.add_argument('--node-name', default=os.getenv('NODE_NAME'))
    parser.add_argument('-n', '--namespace', default=os.getenv('POD_NAMESPACE', 'default'))
    parser.add_argument('--transport', default=os.getenv('NDN_CLIENT_TRANSPORT', 'unix:///run/nfd.sock'))
    parser.add_argument('--function-image', default=os.getenv('FUNCTION_IMAGE', 'ryotaroiwata/my-edge-function:latest'))
    parser.add_argument('-f', '--freshness', type=int, default=0)
    parser.add_argument('-F', '--final', action='store_true')
    parser.add_argument('-w', '--timeout', type=int, help='execution timeout after registration, milliseconds')
    parser.add_argument('--connect-timeout', type=float, default=30, help='NFD connection wait, seconds')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    if args.name is None:
        if not args.node_name:
            parser.error('provide /prefix or set NODE_NAME (registers /NODE_NAME/seed)')
        args.name = f'/{args.node_name}/seed'
    args.name = Name.to_str(Name.normalize(args.name))
    if args.name == '/' or args.freshness < 0 or (args.timeout is not None and args.timeout < 0) or args.connect_timeout < 0:
        parser.error('prefix must be non-root and time values must be nonnegative')
    return args


async def run(args):
    backend = KubernetesBackend(args.namespace, args.node_name, args.function_image)
    app = NDNApp(face=make_face(args.transport), keychain=KeychainDigest())
    server = SeedServer(app, backend, args.name, args.freshness, args.final)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, app.shutdown)

    async def started():
        await server.register()
        if args.timeout is not None:
            loop.call_later(args.timeout / 1000, app.shutdown)

    deadline = loop.time() + args.connect_timeout
    while True:
        try:
            await app.main_loop(after_start=started())
            break
        except (FileNotFoundError, ConnectionError, OSError):
            if loop.time() >= deadline:
                raise
            logging.info('Waiting for NFD: %s', args.transport)
            await asyncio.sleep(0.5)
    if server.tasks:
        await asyncio.gather(*server.tasks)


def main():
    args = arguments()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    try:
        asyncio.run(run(args))
    except Exception:
        logging.exception('Seed stopped with an error')
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
