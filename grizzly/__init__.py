from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version('grizzly-loadtester')
except PackageNotFoundError:
    pass
