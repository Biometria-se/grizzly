#!/usr/bin/env python3
from __future__ import annotations

import sys
from io import StringIO
from json import loads as jsonloads
from pathlib import Path
from typing import Dict, List

import requests
from piplicenses import CustomNamespace, FormatArg, FromArg, OrderArg, create_output_string
from pytablewriter import MarkdownTableWriter

URL_MAP: Dict[str, str] = {}

REPO_ROOT = Path(__file__).parent.parent.resolve()


def generate_license_table() -> List[str]:
    args = CustomNamespace()
    args.format_ = FormatArg.JSON
    args.from_ = FromArg.MIXED
    args.order = OrderArg.LICENSE
    args.summary = False
    args.with_authors = False
    args.with_urls = True
    args.with_description = False
    args.with_license_file = True
    args.no_license_path = False
    args.with_license_file = False
    args.ignore_packages = []
    args.packages = []
    args.fail_on = None
    args.allow_only = None
    args.with_system = False
    args.filter_strings = False
    args.with_maintainers = False
    args.no_version = False
    args.python = sys.executable

    licenses = jsonloads(create_output_string(args))
    headers = ['Name', 'Version', 'License']

    table_contents: List[List[str]] = []

    for license_struct in licenses:
        name = license_struct['Name']
        if name.startswith('grizzly-') or name in ['pkg-resources']:
            continue

        if license_struct['URL'] == 'UNKNOWN':
            try:
                response = requests.get(f'https://pypi.org/pypi/{name}/json', timeout=10)

                if response.status_code != 200:
                    message = f'{response.url} returned {response.status_code}'
                    raise ValueError(message)

                result = jsonloads(response.text)

                info = result.get('info', None) or {}
                project_urls = info.get('project_urls', None) or {}

                url = project_urls.get(
                    'Homepage',
                    project_urls.get(
                        'Home',
                        info.get(
                            'project_url',
                            info.get(
                                'package_url',
                                URL_MAP.get(name),
                            ),
                        ),
                    ),
                )

                if url is None:
                    message = f'no URL found on {response.url} or in static map'
                    raise ValueError(message)

                license_struct['URL'] = url
            except Exception as e:
                print(f'!! you need to find an url for package "{name}": {e!s}', file=sys.stderr)
                sys.exit(1)

        name = f'[{name}]({license_struct["URL"]})'

        table_contents.append([
            name,
            license_struct['Version'],
            license_struct['License'],
        ])

    writer = MarkdownTableWriter(
        headers=headers,
        value_matrix=table_contents,
        margin=1,
    )

    writer.stream = StringIO()
    writer.write_table()

    return ['### Python dependencies\n'] + [f'{row}\n' for row in writer.stream.getvalue().strip().split('\n')]



def generate_native_dependencies_section() -> List[str]:
    section = """
### Native dependencies

Container images (both grizzly runtime and Microsoft Visual Code devcontainer) contains dependencies from
[IBM MQ Redistributable Components](https://www.ibm.com/docs/en/ibm-mq/9.3?topic=information-mq-redistributable-components).

The redistributable license terms may be found in the relevant IBM MQ Program license agreement, which may be found at the
[IBM Software License Agreements](https://www.ibm.com/software/sla/sladb.nsf/search/) website, or in `licenses/` directory
in the [archive](https://ibm.biz/IBM-MQC-Redist-LinuxX64targz).
"""

    return [f'{row.strip()}\n' for row in section.strip().split('\n')]


def main() -> int:
    contents = (REPO_ROOT / 'LICENSE.md').read_text().splitlines()

    license_table = generate_license_table()
    native_dependencies = generate_native_dependencies_section()
    contents[0] = f'#{contents[0]}'
    license_contents = [*contents, '\n', '## Third party licenses\n', '\n', *license_table, '\n', *native_dependencies]

    print(''.join(license_contents))

    return 0


if __name__ == '__main__':
    sys.exit(main())
