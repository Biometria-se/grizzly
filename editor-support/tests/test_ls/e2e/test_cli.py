from __future__ import annotations

from pathlib import Path

from grizzly_ls.utils import run_command


def test_cli_lint() -> None:
    cwd = Path(__file__).parent.parent.parent / 'project'
    rc, output = run_command(
        ['grizzly-ls', 'lint', '.'],
        cwd=cwd,
    )

    try:
        assert rc == 0
    except AssertionError:
        print('\n'.join(output))
        raise
