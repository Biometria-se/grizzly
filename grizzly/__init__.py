from gevent import monkey

monkey.patch_all()

from importlib.metadata import version, PackageNotFoundError
from .__version__ import __version__

try:
    __locust_version__ = version('locust')
except PackageNotFoundError:  # pragma: no coverage
    __locust_version__ = '<unknown>'

__all__ = ['__version__']
