"""Any import from a grizzly module should intialize version (grizzly and locust) variables."""
from gevent import monkey

monkey.patch_all()

from importlib.metadata import PackageNotFoundError, version

from .__version__ import __version__

try:
    __locust_version__ = version('locust')
except PackageNotFoundError:  # pragma: no coverage
    __locust_version__ = '<unknown>'

__all__ = ['__version__']
