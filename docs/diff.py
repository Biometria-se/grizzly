from pathlib import Path

for file_old in Path.cwd().rglob('_build_old/**/*.md'):
    file_new = Path(str(file_old).replace('/_build_old/', '/_build_new/'))

    command = ['vim -d', str(file_old), str(file_new)]
    print(' '.join(command))

