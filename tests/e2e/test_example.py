from tempfile import NamedTemporaryFile
from typing import Optional
from pathlib import Path

import yaml

from ..fixtures import End2EndFixture, Webserver
from ..helpers import run_command


def test_e2e_example(webserver: Webserver, e2e_fixture: End2EndFixture) -> None:
    try:
        result: Optional[str] = None
        with open('example/environments/example.yaml') as env_yaml_file:
            env_conf = yaml.full_load(env_yaml_file)

            for name in ['dog', 'cat', 'book']:
                # @TODO: host needs to be something else when running dist
                env_conf['configuration']['facts'][name]['host'] = f'http://127.0.0.1:{webserver.port}'

        if e2e_fixture._distributed:
            example_root = str((e2e_fixture.root / '..' / 'test-example').resolve())
            command = ['grizzly-cli', 'dist', '--project-name', 'test-example', 'build', '--no-cache', '--local-install']
            print(' '.join(command))
            code, output = run_command(
                command,
                cwd=str(e2e_fixture.mode_root),
                env=e2e_fixture._env,
            )

            try:
                assert code == 0
            except AssertionError:
                print(''.join(output))

                raise
        else:
            example_root = str(Path.cwd() / 'example')

        with NamedTemporaryFile(delete=True, suffix='.yaml', dir=f'{example_root}/environments') as env_conf_file:
            env_conf_file.write(yaml.dump(env_conf, Dumper=yaml.Dumper).encode())
            env_conf_file.flush()

            command = [
                'grizzly-cli',
                e2e_fixture.mode, 'run',
                '--yes',
                '-e', env_conf_file.name,
                'features/example.feature'
            ]
            if e2e_fixture._distributed:
                command = command[:2] + ['--project-name', 'test-example'] + command[2:]

            code, output = run_command(command, env=e2e_fixture._env, cwd=example_root)

            result = ''.join(output)

            assert code == 0
            assert 'ERROR' not in result
            assert 'WARNING' not in result
            assert '1 feature passed, 0 failed, 0 skipped' in result
            assert '3 scenarios passed, 0 failed, 0 skipped' in result
            assert '26 steps passed, 0 failed, 0 skipped, 0 undefined' in result

            assert '''Scenario
ident   iter  status   description
------|-----|--------|---------------|
001      2/2  passed   dog facts api
002      1/1  passed   cat facts api
003      1/1  passed   book api
------|-----|--------|---------------|''' in result

            assert 'executing custom.User.request for get-cat-facts and /facts?limit=' in result

            assert 'sending "server_client" from SERVER' in result
            assert "received from SERVER: msg.node_id='local', msg.data={'server': 'client'}" in result
            assert 'sending "client_server" from CLIENT' in result
            assert "received from CLIENT: msg.node_id='local', msg.data={'client': 'server'}" in result
            assert "AtomicCustomVariable.foobar='foobar'" in result
    except:
        if result is not None:
            print(result)
        raise
