import subprocess

from os import environ


def test_main_no_pymqi() -> None:
    env = environ.copy()
    del env['LD_LIBRARY_PATH']
    env['PYTHONPATH'] = '.'

    process = subprocess.Popen(
        [
            '/usr/bin/env',
            'python3',
            '-c',
            'from grizzly_extras.messagequeue import daemon; print(f"{daemon.pymqi.__name__=}"); daemon.main();'
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    out, _ = process.communicate()
    output = out.decode()
    assert process.returncode == 1
    assert "daemon.pymqi.__name__='grizzly_extras.dummy_pymqi'" in output
    assert 'NotImplementedError: pymqi not installed' in output
