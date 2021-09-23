#!/usr/bin/env python

import sys

from typing import List, Optional
from os import path
from json import loads as jsonloads
from io import StringIO
from datetime import datetime

from piplicenses import CustomNamespace, FormatArg, FromArg, OrderArg, create_output_string
from pytablewriter import MarkdownTableWriter

URL_MAP = {
    'tzlocal': 'https://github.com/regebro/tzlocal',
}

REPO_ROOT = path.realpath(path.join(path.dirname(__file__), '..'))


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

    licenses = jsonloads(create_output_string(args))
    headers = ['Name', 'Version', 'License']

    table_contents: List[List[str]] = []

    for license in licenses:
        name = license['Name']
        if license['URL'] == 'UNKNOWN':
            if name in URL_MAP:
                license['URL'] = URL_MAP[name]
            else:
                print(f'!! you need to find an url for package {name}')
                sys.exit(1)

        name = f'[{name}]({license["URL"]})'

        table_contents.append([
            name,
            license['Version'],
            license['License'],
        ])

    writer = MarkdownTableWriter(
        headers=headers,
        value_matrix=table_contents,
        margin=1,
    )

    writer.stream = StringIO()
    writer.write_table()

    license_table = ['### Python dependencies\n'] + [f'{row}\n' for row in writer.stream.getvalue().strip().split('\n')]

    return license_table

def generate_native_dependencies_section() -> List[str]:
    with open(path.join(REPO_ROOT, '.devcontainer', 'Dockerfile')) as fd:
        contents = fd.readlines()

    mq_version: Optional[str] = None
    mq_url: str

    for line in contents:
        if mq_version is None and line.startswith('ENV MQ_VERSION'):
            _, mq_version, _ = line.split('"')
            continue

        if 'public.dhe.ibm.com' in line:
            _, mq_url, _ = line.strip().split(' ', 2)

        if 'mcr.microsoft.com' in line:
            break

    if mq_version is None:
        print('!! unable to find ENV MQ_VERSION in .devcontainer/Dockerfile')
        sys.exit(1)

    mq_url = mq_url.replace('${MQ_VERSION}', mq_version)

    section = f'''
### Native dependencies

Container images (both grizzly runtime and Microsoft Visual Code devcontainer) contains dependencies from
[IBM MQ Redistributable Components](https://www.ibm.com/docs/en/ibm-mq/9.2?topic=information-mq-redistributable-components).

The redistributable license terms may be found in the relevant IBM MQ Program license agreement, which may be found at the
[IBM Software License Agreements](https://www.ibm.com/software/sla/sladb.nsf/search/) website, or in `licenses/` directory
in the [archive]({mq_url}).
'''

    return [f'{row.strip()}\n' for row in section.strip().split('\n')]


def main() -> int:
    with open(path.join(REPO_ROOT, 'LICENSE.md')) as fd:
        contents = fd.readlines()

    # update copyright year
    for index in range(0, len(contents) - 1):
        line = contents[index]
        if line.startswith('Copyright'):
            parts = line.split(' ')
            documented_year = parts[2]
            current_year = datetime.now().strftime('%Y')

            if documented_year != current_year:
                contents[index] = ' '.join(parts)
                print('!! updating year in LICENSE.md')
                with open(path.join(REPO_ROOT, 'LICENSE.md'), 'w') as fd:
                    fd.writelines(contents)
            break

    license_table = generate_license_table()
    native_dependencies = generate_native_dependencies_section()
    contents[0] = f'#{contents[0]}'
    license_contents = ['# License\n', '\n'] + contents + ['\n', '## Third party licenses\n', '\n'] + license_table + ['\n'] + native_dependencies

    with open(path.join(REPO_ROOT, 'docs', 'licenses.md'), 'w') as fd:
        fd.writelines(license_contents)

    return 0

if __name__ == '__main__':
    sys.exit(main())
