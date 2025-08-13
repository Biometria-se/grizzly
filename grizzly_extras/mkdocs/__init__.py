"""grizzly-mkdocs plugin for dynamically generate the grizzly documentation."""

from __future__ import annotations

import inspect
import re
import warnings
from contextlib import suppress
from copy import deepcopy
from datetime import datetime, timezone
from importlib import import_module
from json import dumps as jsondumps
from pathlib import Path
from textwrap import indent
from typing import TYPE_CHECKING, Any

import mkdocs_gen_files
import yaml
from mkdocs.config import config_options as c
from mkdocs.config.base import Config
from mkdocs.exceptions import ConfigurationError
from mkdocs.plugins import BasePlugin
from mkdocs.structure.files import File, InclusionLevel

from grizzly_extras.mkdocs.log import MkdocsPluginLogger

if TYPE_CHECKING:  # pragma: no cover
    from types import ModuleType

    from jinja2.environment import Environment
    from mkdocs.config.defaults import MkDocsConfig
    from mkdocs.structure.files import Files
    from mkdocs.structure.pages import Page

    from grizzly_extras.mkdocs.postprocessors import PostProcessor, TextFormat


def transform_step_header(text: str, module: str | None) -> str:
    if not text.startswith('step_'):
        return text

    # remove "step" and "module" prefix from function name
    text = re.sub(r'step_.*?_', '', text)

    # remove function arguments
    text = re.sub(r'\(.*', '', text)

    if module is not None:
        _, module = module.rsplit('.', 1)

        text = text.replace(f'{module}_', '')

    # remove snakes, and capatilize
    return text.replace('_', ' ').capitalize()


class _Module(Config):
    output = c.Type(str)
    extra_frontmatter = c.Optional(c.Type(dict))
    extra_mkdocstring_options = c.Optional(c.Type(dict))
    ignore = c.Optional(c.ListOfItems(c.Type(str)))
    postprocessors = c.Optional(c.ListOfItems(c.Type(str)))


class GrizzlyMkdocsConfig(Config):
    modules = c.DictOfItems(c.SubConfig(_Module))


class GrizzlyMkdocs(BasePlugin[GrizzlyMkdocsConfig]):
    docs_dir: Path
    site_dir: Path
    repo_url: str
    generated_files: list[Path]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.logger = MkdocsPluginLogger()
        self.generated_files = []

    def _find_nav(self, module_name: str, nav: list, parent: dict | None = None) -> dict | None:
        for item in nav:
            self.logger.debug(f'find navigation: {module_name}', payload=jsondumps(item, indent=2))

            if isinstance(item, list):
                result = self._find_nav(module_name, item)
                if result is not None:
                    return result

            if isinstance(item, dict):
                for sub_item in item.values():
                    if isinstance(sub_item, list):
                        result = self._find_nav(module_name, sub_item, parent=item)
                        if result is not None:
                            return result

                    if isinstance(sub_item, str) and sub_item == module_name:
                        return item

            if parent is not None and isinstance(item, str) and item == module_name:
                return parent

        return None

    def _make_human_readable(self, text: str) -> str:
        words: list[str] = [word.capitalize() for word in text.split('_')]

        output = ' '.join(words)

        for word in ['http', 'sftp', 'api', 'csv', 'aad', 'json']:
            output = output.replace(word.capitalize(), word.upper())
            output = output.replace(word, word.upper())

        to_replace = {'Iot': 'IoT', 'hub': 'Hub'}
        for value, replace_value in to_replace.items():
            output = output.replace(value, replace_value)

        return output

    def _get_config(self, config: MkDocsConfig, attr: str) -> Any:
        config_file_name = Path(config.config_file_path).name
        value = config.get(attr, None)

        if value is None:
            message = f'`{attr}` is not present in {config_file_name}'
            raise ConfigurationError(message)

        return value

    def _validate_doc(self, source: str, doc: str) -> None:
        old_statements: list[str] = [statement for statement in ['@anchor', '@pylink', '@link', '@shell', '@cat'] if statement in doc]

        if old_statements:
            old_statements_string = ', '.join(old_statements)
            self.logger.error(f'{source} contains pydoc-markdown {old_statements_string} statements')

    def on_config(self, config: MkDocsConfig) -> MkDocsConfig | None:
        for attr in ['docs_dir', 'site_dir']:
            setattr(self, attr, Path(self._get_config(config, attr)).absolute())

        self.repo_url = self._get_config(config, 'repo_url')

        return config

    def _on_build(self, *, missing_ok: bool) -> None:
        for file in self.generated_files:
            file.unlink(missing_ok=missing_ok)

    def on_pre_build(self, config: MkDocsConfig) -> None:  # noqa: ARG002
        self._on_build(missing_ok=True)

    def on_post_build(self, config: MkDocsConfig) -> None:  # noqa: ARG002
        self._on_build(missing_ok=False)
        self.logger.trace(f'removed generated files in {self.docs_dir}')

    def _get_members(self, py_module: ModuleType) -> list[str]:
        members: list[str] = []

        module_members = inspect.getmembers(py_module)
        for name, possible_member in module_members:
            if name.startswith('_'):
                continue

            actual_py_module = inspect.getmodule(possible_member)

            if actual_py_module != py_module:
                continue

            member = possible_member.__wrapped__ if hasattr(possible_member, '__wrapped__') else possible_member

            if not (inspect.isfunction(member) or inspect.ismethod(member) or inspect.isclass(member)):
                continue

            if member.__doc__ is None:
                continue

            self._validate_doc(f'{py_module.__name__}::{member.__name__}', member.__doc__)

            members.append(name)

        return members

    def _scan_module(
        self,
        root_module: Path,
        root_module_name: str,
        output_path: Path,
        files: Files,
        module_conf: Config,
        *,
        is_package: bool,
        parent_title: str,
    ) -> list[str | dict[str, str]] | str:
        nav_items: list[str | dict[str, str]] | str = [] if root_module.is_dir() else ''

        extra_frontmatter: dict | None = module_conf.get('extra_frontmatter')
        extra_mkdocstrings_options: dict | None = module_conf.get('extra_mkdocstrings_options')
        ignore: list[str] | None = module_conf.get('ignore', None)

        if extra_mkdocstrings_options is None:
            extra_mkdocstrings_options = {}

        if is_package:
            scan_files = list(root_module.glob('*.py'))
        else:
            _, file_name = root_module_name.rsplit('.', 1)
            scan_files = [root_module / f'{file_name}.py']

        log_scan_files = indent('\n'.join([f.as_posix() for f in scan_files]), '  - ')

        self.logger.trace(f'generating documentation for {root_module_name} in {root_module.as_posix()} into {output_path.as_posix()}', payload=log_scan_files)

        for path in sorted(scan_files):
            relative_path = path.absolute().relative_to(root_module)

            if ignore is not None and relative_path.as_posix() in ignore:
                continue

            module_path = relative_path.with_suffix('')
            doc_path = path.absolute().relative_to(root_module).with_suffix('.md')
            if doc_path.stem == '__init__':
                doc_path = doc_path.with_stem('index')

            full_doc_path = Path.joinpath(output_path, doc_path)
            relative_doc_path = full_doc_path.relative_to(self.docs_dir)

            parts = (*root_module_name.split('.'), *tuple(module_path.parts))

            if parts[-1] == '__init__' or not is_package:
                parts = parts[:-1]
            elif parts[-1] == '__main__':
                continue

            module_name = '.'.join(parts)
            from_module_name = '.'.join(parts[:-1])

            self.logger.debug(f'importing {module_name} from {from_module_name}')

            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore')
                    py_module = import_module(module_name, package=from_module_name)
                    members: list[str] = []
                    module_extra_mkdocstrings_options = deepcopy(extra_mkdocstrings_options)
                    module_docs: str | None = py_module.__doc__

                    if py_module.__file__ is None:
                        raise FileNotFoundError(py_module.__file__)

                    py_module_file = Path(py_module.__file__)

                    if module_docs is None:
                        continue

                    self._validate_doc(f'{root_module_name.replace(".", "/")}/{relative_path.as_posix()}', module_docs)

                    if 'members' not in module_extra_mkdocstrings_options:
                        members.extend(self._get_members(py_module))

                    log_members = indent('\n'.join(members), '  - ')
                    self.logger.debug(f'{py_module_file.as_posix()} members', payload=log_members)

                    full_doc_path.unlink(missing_ok=True)

                    title = parent_title if doc_path.stem == 'index' else self._make_human_readable(doc_path.stem)
                    date = datetime.fromtimestamp(py_module_file.lstat().st_mtime, tz=timezone.utc)

                    # clean up
                    del py_module

                    module_extra_mkdocstrings_options.update({'members': members or False})

                    frontmatter_markdown = yaml.safe_dump(extra_frontmatter) if extra_frontmatter else '\n'
                    mkdocstrings_options = yaml.safe_dump(module_extra_mkdocstrings_options, indent=4)

                    with mkdocs_gen_files.open(full_doc_path, 'w') as fd:
                        print(
                            f"""---
title: {title}
date: {date.isoformat()}
root_module: {root_module_name}
module: {module_name}
source_url: {self.repo_url}/blob/main/{path.relative_to(Path.cwd()).as_posix()}
{frontmatter_markdown}---
::: {module_name}
    options:
{indent(mkdocstrings_options, prefix=' ' * 8)}
""",
                            file=fd,
                        )

                    mkdocs_gen_files.set_edit_path(full_doc_path, path)

                    self.logger.debug(f'{full_doc_path.as_posix()}', payload=full_doc_path.read_text())

                    self.generated_files.append(full_doc_path)

                    if isinstance(nav_items, list):
                        if relative_doc_path.stem == 'index':
                            nav_items.insert(0, relative_doc_path.as_posix())
                        else:
                            nav_items.append({title: relative_doc_path.as_posix()})
                    else:
                        nav_items = relative_doc_path.as_posix()

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

            if output_path_name is None:
                message = f'{root_module_name} does not have output property defined'
                raise ConfigurationError(message)

            root_module_path_parts: list[str] = root_module_name.split('.')
            root_module = Path.joinpath(Path.cwd(), *root_module_path_parts)
            output_path = Path.joinpath(self.docs_dir, output_path_name)
            is_package = True

            if not root_module.is_dir():
                root_module = root_module.parent
                is_package = False

            nav = self._find_nav(root_module_name, config.nav or [])

            self.logger.debug(f'{root_module_name} nav item', payload=jsondumps(nav, indent=2))

            if nav is None:
                message = f'could not find reference to {root_module_name} in mkdocs.yaml nav'
                raise ConfigurationError(message)

            title = next(iter(nav.keys()))
            nav_items = self._scan_module(root_module, root_module_name, output_path, files, module_conf, is_package=is_package, parent_title=title)

            title_nav = nav.get(title, None)
            if isinstance(title_nav, list) and isinstance(nav_items, list):
                replace_nav_items: list[dict[str, str] | str] = []
                for nav_item in title_nav:
                    if isinstance(nav_item, str) and nav_item == root_module_name:
                        replace_nav_items.extend(nav_items)
                    else:
                        replace_nav_items.append(nav_item)

                # make sure .../index.md is at the first position
                with suppress(ValueError):
                    index_page_path = Path.joinpath(output_path, 'index.md').relative_to(self.docs_dir).as_posix()
                    index = replace_nav_items.index(index_page_path)
                    if index > 0:
                        index_page = replace_nav_items.pop(index)
                        replace_nav_items.insert(0, index_page)

                nav_items = replace_nav_items

            nav.update({title: nav_items})

        self.logger.debug('mkdocs nav:', payload=jsondumps(config.nav, indent=2))

        return files

    def on_page_markdown(self, markdown: str, page: Page, config: MkDocsConfig, files: Files) -> str | None:
        return self._on_page(markdown, page, config, files, text_format='markdown')

    def on_page_content(self, html: str, page: Page, config: MkDocsConfig, files: Files) -> str | None:
        return self._on_page(html, page, config, files, text_format='html')

    def _on_page(self, content: str, page: Page, config: MkDocsConfig, files: Files, *, text_format: TextFormat) -> str | None:  # noqa: ARG002
        module = page.meta.get('root_module', None)

        if module is None:
            return content

        module_conf = self.config.get('modules', {}).get(module, None)

        if module_conf is None:
            return content

        postprocessors: list[str] | None = module_conf.get('postprocessors', None)

        if postprocessors is None:
            return content

        pp_content: str | None = content

        for postprocessor_name in postprocessors:
            module_name, class_name = postprocessor_name.rsplit('.', 1)
            try:
                postprocessor_module = import_module(module_name, package=module_name)
                postprocessor_class: type[PostProcessor] = getattr(postprocessor_module, class_name)
                if postprocessor_class.text_format != text_format:
                    continue

                postprocessor = postprocessor_class(self.logger)

                pp_content = postprocessor(page, pp_content)
            except ModuleNotFoundError:
                self.logger.exception(f'failed to load postprocessor {postprocessor_name}')

        return pp_content

    def on_env(self, env: Environment, config: MkDocsConfig, files: Files) -> Environment | None:  # noqa: ARG002
        env.filters['transformstepheader'] = transform_step_header

        return env
