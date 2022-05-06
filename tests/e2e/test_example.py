from tempfile import NamedTemporaryFile
from os import chdir, getcwd
from typing import Optional

import yaml

from ..fixtures import Webserver
from ..helpers import run_command


def test_e2e_example(webserver: Webserver) -> None:
    cwd = getcwd()

    try:
        result: Optional[str] = None
        with open('example/environments/example.yaml') as env_yaml_file:
            env_conf = yaml.full_load(env_yaml_file)

            for name in ['dog', 'cat', 'book']:
                env_conf['configuration']['facts'][name]['host'] = f'http://127.0.0.1:{webserver._web_server.server_port}'

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
            ])

            result = ''.join(output)

            assert code == 0
            assert 'ERROR' not in result
            assert 'WARNING' not in result
            assert '1 feature passed, 0 failed, 0 skipped' in result
            assert '3 scenarios passed, 0 failed, 0 skipped' in result
            assert '21 steps passed, 0 failed, 0 skipped, 0 undefined' in result
    except:
        if result is not None:
            print(result)
        raise
    finally:
        chdir(cwd)
