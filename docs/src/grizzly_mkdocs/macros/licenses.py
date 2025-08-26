"""Macro for generating license information."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from io import StringIO
from json import loads as jsonloads
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal, TypeAlias, cast

import requests
from mkdocs_macros.util import trace
from piplicenses import CustomNamespace, FormatArg, FromArg, OrderArg, create_output_string
from pytablewriter import MarkdownTableWriter

from grizzly_mkdocs.macros.command import command

PackageManager: TypeAlias = Literal['uv', 'npm']
PackageName: TypeAlias = str
PackageVersion: TypeAlias = str
PackageDependency: TypeAlias = tuple[PackageName, PackageVersion]


LICENSES: dict[str, str] = {
    'flask-cors': 'MIT',
}


@dataclass(eq=True, frozen=True)
class License:
    package: str
    version: str
    license: str
    url: str | None = None

    @property
    def markdown_package(self) -> str:
        return f'[{self.package}]({self.url})' if self.url else self.package

    def row(self) -> list[str]:
        return [self.markdown_package, self.version, self.license]


def get_package_dependencies(package: str) -> set[PackageDependency]:
    dependencies: set[PackageDependency] = set()

    output = command(['uv', 'tree', '--package', package, '--no-dev', '--locked', '--no-dedupe'])

    for line in output.splitlines():
        dependency_line = re.sub(r'[^\x00-\x7F]+', '', line.replace('(*)', '')).strip()
        try:
            name, version = dependency_line.split(' ', 1)
        except ValueError:
            if not dependency_line.startswith('grizzly-'):
                raise

            continue

        version = version.removeprefix('v')

        dependencies.add((name, version))

    return dependencies


def update_metadata_from_pypi(license: dict[str, str]) -> dict[str, str]:
    name = license['Name']
    url = license.get('URL', 'UNKNOWN').strip()
    license_name = license.get('License', 'UNKNOWN').strip()

    if 'UNKNOWN' in [url, license_name]:
        response = requests.get(f'https://pypi.org/pypi/{name}/json', timeout=10)

        if response.status_code != 200:
            message = f'{response.url} returned {response.status_code}'
            raise ValueError(message)

        result = jsonloads(response.text)
        info = result.get('info', None) or {}

        if url == 'UNKNOWN':
            project_urls: dict[str, str] = cast('dict[str, str]', info.get('project_urls', None) or {})

            url = project_urls.get(
                'Homepage',
                project_urls.get(
                    'Home',
                    info.get(
                        'project_url',
                        info.get(
                            'package_url',
                            f'https://pypi.org/project/{name}/',
                        ),
                    ),
                ),
            )

            license.update({'URL': url})

        if license_name == 'UNKNOWN':
            license_name = info.get('license_expression', None) or LICENSES.get(name, 'UNKNOWN')
            license.update({'License': license_name})

    return license


def get_licenses_uv(package: str) -> set[License]:
    args = CustomNamespace(
        format_=FormatArg.JSON,
        from_=FromArg.MIXED,
        order=OrderArg.LICENSE,
        summary=False,
        with_authors=False,
        with_urls=True,
        with_description=False,
        with_notice_file=False,
        with_license_file=False,
        no_license_path=False,
        ignore_packages=[],
        packages=[],
        fail_on=None,
        allow_only=None,
        with_system=False,
        filter_strings=False,
        with_maintainers=False,
        no_version=False,
        python=sys.executable,
    )

    dependencies = get_package_dependencies(package)

    for dependency, _ in dependencies:
        args.packages.append(dependency)

    args.packages = list(args.packages)

    licenses: set[License] = set()
    for pip_license in jsonloads(create_output_string(args)):
        license = update_metadata_from_pypi(pip_license)
        licenses.add(License(license['Name'], license['Version'], license['License'], license['URL']))

    return licenses


def get_licenses_npm(path: str) -> set[License]:
    licenses: set[License] = set()

    with NamedTemporaryFile() as fd:
        cwd = Path(path)
        _ = command(['npm', 'install'], cwd=cwd)

        _ = command(['npm', 'run', 'licenses', '--', fd.name], cwd=cwd)
        fd.flush()
        fd.seek(0)

        output = jsonloads(fd.read().decode('utf-8').strip())

    for key, value in output.items():
        if value.get('parents', None) in [None, 'Code', 'UNDEFINED']:
            continue

        package, version = key.rsplit('@', 1)

        if any(package.startswith(prefix) for prefix in ['@types/']):
            continue

        license = value.get('licenses', 'UNKNOWN')
        url = value.get('licenseUrl', None)

        license = License(package, version, license, url)

        licenses.add(license)

    return licenses


def licenses(name: str, package: str, package_manager: Literal['uv', 'npm'] = 'uv') -> str:
    match package_manager:
        case 'npm':
            licenses = get_licenses_npm(name)
        case _:
            licenses = get_licenses_uv(package)

    trace(f'{package} ({package_manager}): found licenses for {len(licenses)} dependencies')

    headers = ['Name', 'Version', 'License']
    table_contents: list[list[str]] = [license.row() for license in sorted(licenses, key=lambda license: license.package.lower())]

    writer = MarkdownTableWriter(
        headers=headers,
        value_matrix=table_contents,
        margin=1,
    )

    writer.stream = StringIO()
    writer.write_table()

    license_file = Path(__file__).parent.parent.parent.parent.parent / 'LICENSE.md'

    markdown: list[str] = []

    if license_file.exists():
        markdown.append(f"""## Package
{license_file.read_text().replace('# ', '### ', 1)}
""")

    markdown.append(f"""## Package dependencies
{writer.stream.getvalue()}
""")

    extra_license_file = Path(__file__).parent.parent.parent.parent.parent / name / 'LICENSE.md'

    if extra_license_file.exists():
        markdown.append(extra_license_file.read_text())

    return ''.join(markdown)
