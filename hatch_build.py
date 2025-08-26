"""Hatch build hook for installing editor-support/client/vscode."""

from __future__ import annotations

import subprocess
from os import environ
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from hatchling.metadata.plugin.interface import MetadataHookInterface


class BuildGrizzly(BuildHookInterface):
    def _build_client(self) -> None:
        if environ.get('SKIP_BUILD_CLIENT', None) is None:
            try:
                target = 'clients/vscode'
                print(f'Building {target}')
                subprocess.check_output('npm install', cwd=target, shell=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                message = f'"{e.cmd}" got exit code {e.returncode}: {e.output}'
                raise RuntimeError(message) from e

    def is_dynamic_readme(self) -> bool:
        return 'readme' in self.metadata.dynamic

    def _finialize_dynamic_readme(self) -> None:
        if not self.is_dynamic_readme():
            return

        readme = Path.cwd() / 'README.md'
        readme.unlink()

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        match self.metadata.name:
            case 'grizzly-loadtester-ls':
                self._build_client()
            case _:
                pass

        return super().initialize(version, build_data)

    def finalize(self, version: str, build_data: dict[str, Any], artifact_path: str) -> None:
        self._finialize_dynamic_readme()

        super().finalize(version, build_data, artifact_path)


class MetadataGrizzly(MetadataHookInterface):
    def _update_readme(self, metadata: dict) -> None:
        if 'readme' not in metadata.get('dynamic', []):
            return

        readme_source = Path.cwd().parent / 'README.md'
        readme_dest = Path.cwd() / 'README.md'
        readme_dest.unlink(missing_ok=True)

        if not readme_source.exists():
            message = f'{readme_source.as_posix()} does not exist'
            raise FileNotFoundError(message)

        readme_dest.write_text(readme_source.read_text())

        metadata.update({'readme': 'README.md'})

    def update(self, metadata: dict) -> None:
        self._update_readme(metadata)
