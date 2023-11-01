#!/usr/bin/env python
import sys
import subprocess

from pathlib import Path
from shlex import split
from time import perf_counter

import yaml


def main() -> int:
    workspace_root = (Path(__file__).parent / '..').resolve()
    workflow_file = workspace_root / '.github' / 'workflows' / 'code-quality.yaml'

    assert workflow_file.exists(), 'could not find code quality workflow'

    workflow_yaml = yaml.load(workflow_file.read_bytes(), Loader=yaml.Loader)

    linting_steps = workflow_yaml.get('jobs', {}).get('linting', {}).get('steps', [])

    main_rc: int = 0

    for linting_step in linting_steps:
        name = linting_step.get('name', None)
        run = linting_step.get('run', None)

        if run is None or 'pip install' in run:
            continue

        # change pylint argument
        run = run.replace('python', 'python3')

        print(f'{name}: {run}', file=sys.stderr, end=' ')
        sys.stderr.flush()

        command = split(run)

        start = perf_counter()
        # process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        rc = subprocess.call(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=str(workspace_root), shell=False)
        # stdout, stderr = process.communicate()
        # rc = process.wait()
        delta = (perf_counter() - start) * 1000
        print(f', took {delta} ms', file=sys.stderr)
        sys.stderr.flush()
        main_rc += rc

        """
        if rc != 0:
            print(stdout.decode('utf-8'), file=sys.stderr)
            print(stderr.decode('utf-8'), file=sys.stderr)
        """

    return main_rc


if __name__ == '__main__':
    sys.exit(main())
