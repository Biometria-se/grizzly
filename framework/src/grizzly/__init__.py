"""Any import from a grizzly module should intialize version (grizzly and locust) variables."""

from gevent import monkey

monkey.patch_all()

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version('grizzly-loadtester')
except PackageNotFoundError:
    __version__ = 'unknown'

try:
    __common_version__ = version('grizzly-loadtester-common')
except PackageNotFoundError:
    __common_version__ = 'unknown'

try:
    __locust_version__ = version('locust')
except PackageNotFoundError:  # pragma: no cover
    __locust_version__ = 'unknown'
