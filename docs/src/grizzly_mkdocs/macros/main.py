"""Macros for MkDocs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from grizzly_mkdocs.macros.changelog import changelog
from grizzly_mkdocs.macros.command import command
from grizzly_mkdocs.macros.licenses import licenses

if TYPE_CHECKING:
    from mkdocs_macros.plugin import MacrosPlugin


def define_env(env: MacrosPlugin) -> None:
    env.macro(licenses)
    env.macro(changelog)
    env.macro(command)
