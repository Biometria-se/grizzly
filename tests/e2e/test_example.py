from tempfile import NamedTemporaryFile
from os import chdir, getcwd
from typing import Optional

import yaml

from ..fixtures import BehaveContextFixture, Webserver
from ..helpers import run_command


def test_e2e_example(webserver: Webserver, behave_context_fixture: BehaveContextFixture) -> None:
    cwd = getcwd()

    try:
        result: Optional[str] = None
        with open('example/environments/example.yaml') as env_yaml_file:
            env_conf = yaml.full_load(env_yaml_file)

            for name in ['dog', 'cat', 'book']:
                env_conf['configuration']['facts'][name]['host'] = f'http://127.0.0.1:{webserver.port}'

        with NamedTemporaryFile(delete=True, suffix='.yaml') as env_conf_file:
            chdir('example/')
            env_conf_file.write(yaml.dump(env_conf, Dumper=yaml.Dumper).encode())
            env_conf_file.flush()

            code, output = run_command([
                'grizzly-cli',
                'local', 'run',
                '--yes',
                '-e', env_conf_file.name,
                'features/example.feature'
            ], env=behave_context_fixture._env)

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
    finally:
        chdir(cwd)
