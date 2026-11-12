"""Version qualification; never imports credentials or contacts a venue."""
from importlib.metadata import PackageNotFoundError, version
import sys

def require_official_nautilus():
    if sys.implementation.name != 'cpython' or sys.version_info[:2] not in ((3, 13), (3, 14)):
        raise RuntimeError('This adapter candidate requires CPython 3.13 or3.14')
    try:
        actual = version('nautilus_trader')
    except PackageNotFoundError:
        raise ImportError('Install the official nautilus_trader==2.0.0rc7 wheel') from None
    if actual != '2.0.0rc7':
        raise RuntimeError('Adapter requires nautilus_trader==2.0.0rc7; other APIs are unqualified')
