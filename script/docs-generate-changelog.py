import sys
import subprocess

from os import path

def main() -> int:
    output = subprocess.check_output(['git', 'tag']).decode('utf-8').strip()

    base_directory = path.realpath(path.join(path.dirname(__file__), '..', 'docs'))

    tags = output.split('\n')
    tags.sort(reverse=True)

    print(f'{tags=}')

    with open(path.join(base_directory, 'changelog.md'), 'w') as fd:
        fd.write('# Changelog\n\n')

        for index, previous_tag in enumerate(tags[1:], start=1):
            current_tag = tags[index-1]
            print(f'generating changelog for {current_tag} <- {previous_tag}')

            output = subprocess.check_output([
                'git',
                'log',
                f"{previous_tag}...{current_tag}",
                '--oneline',
                '--no-abbrev',
                '--no-merges',
            ]).decode('utf-8').strip()

            fd.write(f'## {current_tag}\n\n')

            for line in output.split('\n'):
                commit = line[:40]
                commit_short = commit[:8]
                message = line[41:].strip()

                fd.write(f'* <a href="https://github.com/Biometria-se/grizzly/commit/{commit}" target="_blank">`{commit_short}`</a>: {message}\n\n')

            fd.write('\n')

    return 0


if __name__ == '__main__':
    sys.exit(main())
