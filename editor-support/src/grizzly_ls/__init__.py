from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version('grizzly-loadtester-ls')
except PackageNotFoundError:  # pragma: no cover
    __version__ = 'unknown'
