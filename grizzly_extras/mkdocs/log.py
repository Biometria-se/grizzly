"""grizzly-mkdocs logger."""

from __future__ import annotations

import logging

from mkdocs.plugins import get_plugin_logger
from termcolor import colored


class MkdocsPluginLogger:
    trace_color: str = 'yellow'

    def __init__(self, name: str = 'grizzly') -> None:
        self.logger = get_plugin_logger(name)
        self.trace('loading')

    def format_message(self, *args: str, payload: str = '') -> str:
        first = args[0]
        rest = list(args[1:])

        if payload:
            rest.append(f'\n{payload}')

        text = f'{first}'
        emphasized = colored(text, self.trace_color)
        return ' '.join([emphasized, *rest])

    def trace(self, *args: str, payload: str = '', level: int = logging.INFO) -> None:
        msg = self.format_message(*args, payload=payload)
        self.logger.log(level, msg)

    def debug(self, *args: str, payload: str = '') -> None:
        self.trace(*args, payload, level=logging.DEBUG)

    def info(self, *args: str, payload: str = '') -> None:
        self.trace(*args, payload, level=logging.INFO)

    def warning(self, *args: str, payload: str = '') -> None:
        self.trace(*args, payload, level=logging.WARNING)

    def error(self, *args: str, payload: str = '') -> None:
        self.trace(*args, payload, level=logging.ERROR)

    def exception(self, *args: str, payload: str = '') -> None:
        msg = self.format_message(*args, payload=payload)
        self.logger.exception(msg)
