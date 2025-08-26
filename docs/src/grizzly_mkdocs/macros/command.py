"""Macro for executing commands."""

import subprocess
from pathlib import Path


def command(cmd: str | list[str], *, cwd: str | Path | None = None) -> str:
    shell = isinstance(cmd, str)

    if isinstance(cwd, str):
        cwd = Path(cwd)

    output = subprocess.run(cmd, check=False, capture_output=True, shell=shell, cwd=cwd)

    return output.stdout.decode('utf-8').strip()
