"""Fetch only the public, hash-pinned helper source during the image build."""
import hashlib
from pathlib import Path
import tarfile
import urllib.request

VERSION = '3.5.9'
SHA256 = '603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a'
URL = 'https://www.openssl.org/source/openssl-'+VERSION+'.tar.gz'


def fetch():
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(URL, timeout=60) as response:
        raw = response.read(64*1024*1024+1)
    if len(raw) > 64*1024*1024 or hashlib.sha256(raw).hexdigest() != SHA256:
        raise ValueError('OpenSSL source digest mismatch')
    archive = Path('/build/openssl.tar.gz')
    archive.write_bytes(raw)
    with tarfile.open(archive) as source:
        source.extractall('/build', filter='data')
    archive.unlink()


if __name__ == '__main__': fetch()
