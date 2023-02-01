from tempfile import NamedTemporaryFile
from typing import Optional
from pathlib import Path
from getpass import getuser

import yaml

from ..fixtures import End2EndFixture
from ..helpers import run_command


def test_e2e_example(e2e_fixture: End2EndFixture) -> None:
    try:
        result: Optional[str] = None

        with open('example/environments/example.yaml') as env_yaml_file:
            env_conf = yaml.full_load(env_yaml_file)

            for name in ['dog', 'cat', 'book']:
                env_conf['configuration']['facts'][name]['host'] = f'http://{e2e_fixture.host}'

        if e2e_fixture._distributed:
            example_root = str((e2e_fixture.root / '..' / 'test-example').resolve())
            command = ['grizzly-cli', 'dist', '--project-name', 'test-example', 'build', '--no-cache', '--local-install']
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

            root = (Path(__file__) / '..' / '..' / '..').resolve()
            feature_file_root = str(example_root).replace(f'{root}/', '')
            feature_file = f'{feature_file_root}/features/example.feature'
            feature_file_contents = Path(feature_file).read_text().split('\n')

            index = feature_file_contents.index('  Scenario: dog facts api')
            # should go last in "Background"-section
            feature_file_contents.insert(index - 1, f'    Then start webserver on master port "{e2e_fixture.webserver.port}"')

            with open(feature_file, 'w') as fd:
                fd.truncate(0)
                fd.write('\n'.join(feature_file_contents))

            exec_root = str(e2e_fixture.mode_root)
        else:
            example_root = exec_root = str(Path.cwd() / 'example')
            feature_file = 'features/example.feature'

        with NamedTemporaryFile(delete=True, suffix='.yaml', dir=f'{example_root}/environments') as env_conf_file:
            env_conf_file.write(yaml.dump(env_conf, Dumper=yaml.Dumper).encode())
            env_conf_file.flush()

            command = [
                'grizzly-cli',
                e2e_fixture.mode, 'run',
                '--yes',
                '--verbose',
                '-e', env_conf_file.name,
                feature_file,
            ]

            if e2e_fixture._distributed:
                command = command[:2] + ['--project-name', 'test-example'] + command[2:]

            code, output = run_command(
                command,
                env=e2e_fixture._env,
                cwd=exec_root,
            )

            result = ''.join(output)

            if e2e_fixture._distributed:
                result = ''
                for container in ['master', 'worker']:
                    command = ['docker', 'container', 'logs', f'test-example-{getuser()}_{container}_1']
                    _, output = run_command(
                        command,
                        cwd=exec_root,
                        env=e2e_fixture._env,
                    )

                    result += ''.join(output)

                if code != 0:
                    print(''.join(output))

            assert code == 0
            assert 'ERROR' not in result
            assert 'WARNING' not in result
            assert '1 feature passed, 0 failed, 0 skipped' in result
            assert '3 scenarios passed, 0 failed, 0 skipped' in result
            # Then start webserver... added when running distributed
            step_count = 26 if e2e_fixture._distributed else 25
            assert f'{step_count} steps passed, 0 failed, 0 skipped, 0 undefined' in result

            assert 'ident   iter  status   description' in result
            assert '001      2/2  passed   dog facts api' in result
            assert '002      1/1  passed   cat facts api' in result
            assert '003      1/1  passed   book api' in result
            assert '------|-----|--------|---------------|' in result

            assert 'executing custom.User.request for get-cat-facts and /facts?limit=' in result

            assert 'sending "client_server" from CLIENT' in result
            assert "received from CLIENT" in result
            assert "AtomicCustomVariable.foobar='foobar'" in result

            # check debugging and that task index -> step expression is correct
            assert 'executing task 1 of 3: iterator' in result
            assert (
                'executing task 2 of 3: Then get request with name "get-dog-facts" from endpoint '
                '"/api/v1/resources/dogs?number={{ AtomicRandomInteger.dog_facts_count }}'
            ) in result
            assert 'executing task 3 of 3: pace' in result

            assert 'executing task 1 of 4: iterator' in result
            assert 'executing task 2 of 4: Then get request with name "get-cat-facts" from endpoint "/facts?limit={{ AtomicRandomInteger.cat_facts_count }}"' in result
            assert 'executing task 3 of 4: And send message "{\'client\': \'server\'}"' in result
            assert 'executing task 4 of 4: pace' in result

            assert 'executing task 1 of 5: iterator' in result
            assert 'executing task 2 of 5: Then get request with name "1-get-book" from endpoint "/books/{{ AtomicCsvRow.books.book }}.json | content_type=json"' in result
            assert 'executing task 3 of 5: Then get request with name "2-get-author" from endpoint "{{ author_endpoint }}.json | content_type=json"' in result
            assert 'executing task 4 of 5: Then log message "AtomicCustomVariable.foobar=\'{{ steps.custom.AtomicCustomVariable.foobar }}\'"' in result
            assert 'executing task 5 of 5: pace' in result
    except:
        if result is not None:
            print(result)
        raise
