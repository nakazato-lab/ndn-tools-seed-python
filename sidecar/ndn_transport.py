"""Use the same NFD endpoint for the runtime and the NDN sidecar."""
import os
from urllib.parse import urlparse
from ndn.app import NDNApp
from ndn.security import KeychainDigest
from ndn.transport.stream_face import TcpFace, UnixFace


class RemoteTcpFace(TcpFace):
    def isLocalFace(self):
        return False


def make_app():
    uri = urlparse(os.getenv('NDN_CLIENT_TRANSPORT', 'unix:///run/nfd.sock'))
    if uri.scheme == 'unix' and uri.path:
        face = UnixFace(uri.path)
    elif uri.scheme in ('tcp', 'tcp4', 'tcp6') and uri.hostname:
        face = RemoteTcpFace(uri.hostname, uri.port or 6363)
    else:
        raise ValueError('NDN_CLIENT_TRANSPORT must be unix:///path or tcp://host:port')
    return NDNApp(face=face, keychain=KeychainDigest())
