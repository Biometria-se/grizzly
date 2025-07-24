"""grizzly-mkdocs plugin for dynamically generate the grizzly documentation."""

from __future__ import annotations

import logging
import warnings
from datetime import datetime, timezone
from importlib import import_module
from json import dumps as jsondumps
from pathlib import Path
from typing import TYPE_CHECKING, Any

import mkdocs_gen_files
import yaml
from mkdocs.config import config_options as c
from mkdocs.config.base import Config
from mkdocs.exceptions import ConfigurationError
from mkdocs.plugins import BasePlugin, get_plugin_logger
from mkdocs.structure.files import File, InclusionLevel
from mkdocs.structure.pages import Page
from termcolor import colored

if TYPE_CHECKING:  # pragma: no cover
    from mkdocs.config.defaults import MkDocsConfig
    from mkdocs.structure.files import Files


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

    def warning(self, *args: str, payload: str = '') -> None:
        self.trace(*args, payload, level=logging.WARNING)

    def exception(self, *args: str, payload: str = '') -> None:
        msg = self.format_message(*args, payload=payload)
        self.logger.exception(msg)


class _Module(Config):
    output = c.Type(str)
    extra_frontmatter = c.Type(dict)


class GrizzlyMkdocsConfig(Config):
    modules = c.DictOfItems(c.SubConfig(_Module))


class GrizzlyMkdocs(BasePlugin[GrizzlyMkdocsConfig]):
    docs_dir: Path
    site_dir: Path
    generated_files: list[Path]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.logger = MkdocsPluginLogger()
        self.generated_files = []

    def _find_nav(self, module_name: str, nav: list) -> dict | None:
        for item in nav:
            if isinstance(item, list):
                return self._find_nav(module_name, item)

            if isinstance(item, dict):
                for sub_item in item.values():
                    if isinstance(sub_item, list):
                        return self._find_nav(module_name, sub_item)

                    if isinstance(sub_item, str) and sub_item == module_name:
                        return item

        return None

    def _make_human_readable(self, text: str) -> str:
        words: list[str] = [word.capitalize() for word in text.split('_')]

        output = ' '.join(words)

        for word in ['http', 'sftp', 'api', 'csv']:
            output = output.replace(word.capitalize(), word.upper())
            output = output.replace(word, word.upper())

        to_replace = {'Iot': 'IoT', 'hub': 'Hub'}
        for value, replace_value in to_replace.items():
            output = output.replace(value, replace_value)

        return output

    def on_config(self, config: MkDocsConfig) -> MkDocsConfig | None:
        config_file_name = Path(config.config_file_path).name

        for attr in ['docs_dir', 'site_dir']:
            value = config.get(attr, None)
            if value is None:
                message = f'`{value}` is not present in {config_file_name}'
                raise ConfigurationError(message)

            setattr(self, attr, Path(value).absolute())

        return config

    def _on_build(self, _config: MkDocsConfig) -> None:
        for file in self.generated_files:
            file.unlink()

        for file in self.docs_dir.rglob('*.gen.md'):
            file.unlink()

    def on_pre_build(self, config: MkDocsConfig) -> None:
        self._on_build(config)

    def on_post_build(self, config: MkDocsConfig) -> None:
        self._on_build(config)
        self.logger.trace(f'removed generated files in {self.docs_dir}')

    def _scan_module(self, root_module: Path, root_module_name: str, output_path: Path, files: Files, extra_frontmatter: dict | None) -> list[str]:
        nav_items: list[str] = []

        for path in sorted(root_module.rglob('*.py')):
            relative_path = path.absolute().relative_to(root_module)
            module_path = relative_path.with_suffix('')
            doc_path = path.absolute().relative_to(root_module).with_suffix('.md')
            if doc_path.stem == '__init__':
                doc_path = doc_path.with_stem('index')

            full_doc_path = Path.joinpath(output_path, doc_path)
            relative_doc_path = full_doc_path.relative_to(self.docs_dir)

            parts = (root_module_name, *tuple(module_path.parts))

            if parts[-1] == '__init__':
                parts = parts[:-1]
            elif parts[-1] == '__main__':
                continue

            module_name = '.'.join(parts)
            from_module_name = '.'.join(parts[:-1])

            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore')
                    py_module = import_module(module_name, package=from_module_name)
                    module_docs: str | None = py_module.__doc__
                    if py_module.__file__ is None:
                        raise FileNotFoundError(py_module.__file__)

                    py_module_file = Path(py_module.__file__)

                    if module_docs is None:
                        continue

                    if module_docs.lower().startswith('emtpy module initializer'):
                        continue

                    # @TODO: check if any functions, methods, classes

                    old_statements: list[str] = [statement for statement in ['@anchor', '@pylink', '@link'] if statement in module_docs]

                    if old_statements:
                        old_statements_string = ', '.join(old_statements)
                        self.logger.warning(f'{root_module_name.replace(".", "/")}/{relative_path.as_posix()} contains pydoc-markdown {old_statements_string} statements')

                    full_doc_path.unlink(missing_ok=True)

                    title = self._make_human_readable(doc_path.stem.replace('.gen', ''))
                    date = datetime.fromtimestamp(py_module_file.lstat().st_mtime, tz=timezone.utc)

                    # clean up
                    del py_module

                    frontmatter_markdown = yaml.safe_dump(extra_frontmatter) if extra_frontmatter else '\n'

                    with mkdocs_gen_files.open(full_doc_path, 'w') as fd:
                        print(
                            f"""---
title: {title}
date: {date.strftime('%Y-%m-%d %H:%M:%S')}
{frontmatter_markdown}---
::: {module_name}
    options:
        members: no
""",
                            file=fd,
                        )

                    mkdocs_gen_files.set_edit_path(full_doc_path, path)
                    self.logger.trace(f'{full_doc_path.as_posix()}', payload=full_doc_path.read_text())

                    self.generated_files.append(full_doc_path)

                    if module_name.endswith('__init__'):
                        nav_items.insert(0, relative_doc_path.as_posix())
                    else:
                        nav_items.append(relative_doc_path.as_posix())

                    files.append(
                        File(
                            relative_doc_path.as_posix(),
                            src_dir=self.docs_dir.as_posix(),
                            dest_dir=self.site_dir.as_posix(),
                            use_directory_urls=True,
                            inclusion=InclusionLevel.INCLUDED,
                        ),
                    )

            except ImportError as e:
                message = str(e)
                self.logger.exception(message, payload=f'{from_module_name=}, {module_name=}')

        return nav_items

    def on_files(self, files: Files, config: MkDocsConfig) -> Files:
        modules_conf: dict[str, Config] = self.config.get('modules', {})

        for root_module_name, module_conf in modules_conf.items():
            output_path_name = module_conf.get('output')
            extra_frontmatter: dict | None = module_conf.get('extra_frontmatter')

            if output_path_name is None:
                message = f'{root_module_name} does not have output property defined'
                raise ConfigurationError(message)

            root_module_path_parts: list[str] = root_module_name.split('.')
            root_module = Path.joinpath(Path.cwd(), *root_module_path_parts).absolute()
            output_path = Path.joinpath(self.docs_dir, output_path_name)

            self.logger.trace(f'generating documentation for {root_module.as_posix()} into {output_path.as_posix()}')
            nav = self._find_nav(root_module_name, config.nav or [])

            if nav is None:
                message = f'could not find reference to {root_module_name} in mkdocs.yaml nav'
                raise ConfigurationError(message)

            key = next(iter(nav.keys()))

            nav_items = self._scan_module(root_module, root_module_name, output_path, files, extra_frontmatter)

            nav.update({key: nav_items})

        return files
