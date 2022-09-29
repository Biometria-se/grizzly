from importlib.metadata import version, PackageNotFoundError
from .__version__ import __version__

try:
    __locust_version__ = version('locust')
except PackageNotFoundError:
    __locust_version__ = '<unknown>'

__all__ = ['__version__']
