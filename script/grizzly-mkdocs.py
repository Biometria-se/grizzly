import sys

from mkdocs.__main__ import cli

sys.exit(cli(['build', '--strict'], standalone_mode=False))
