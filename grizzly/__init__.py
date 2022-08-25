from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version('grizzly-loadtester')
    __locust_version__ = version('locust')
except PackageNotFoundError:
    pass
