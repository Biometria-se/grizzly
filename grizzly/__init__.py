from importlib.metadata import version, PackageNotFoundError

try:
    from .__version__ import __version__
except ModuleNotFoundError:
    __version__ = '<unknown>'

try:
    __locust_version__ = version('locust')
except PackageNotFoundError:
    __locust_version__ = '<unknown>'

__all__ = ['__version__']
