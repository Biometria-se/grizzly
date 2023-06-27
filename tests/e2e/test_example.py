from typing import Optional
from pathlib import Path
from shutil import copytree

import yaml

from tests.fixtures import End2EndFixture


def test_e2e_example(e2e_fixture: End2EndFixture) -> None:
    try:
        result: Optional[str] = None

        with open('example/environments/example.yaml') as env_yaml_file:
            env_conf = yaml.full_load(env_yaml_file)

            for name in ['dog', 'cat', 'book']:
                env_conf['configuration']['facts'][name]['host'] = f'http://{e2e_fixture.host}'

        if e2e_fixture._distributed:
            # copy examples
            source = (Path(__file__) / '..' / '..' / '..' / 'example').resolve()
            example_root = e2e_fixture.root.parent / 'test-example'
            copytree(source, example_root, dirs_exist_ok=True)

            with open(Path(example_root) / 'features' / 'steps' / 'steps.py', 'a') as fd:
                fd.write(
                    e2e_fixture.step_start_webserver.format('/srv/grizzly')
                )

            # create steps/webserver.py
            webserver_source = e2e_fixture.test_tmp_dir.parent / 'tests' / 'webserver.py'
            webserver_destination = example_root / 'features' / 'steps' / 'webserver.py'
            webserver_destination.write_text(webserver_source.read_text())

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
        else:
            example_root = Path.cwd() / 'example'

        feature_file = 'features/example.feature'

        # use test-example project stucture, but with original project name (image)
        original_root = e2e_fixture.root
        e2e_fixture._root = example_root

        code, output = e2e_fixture.execute(feature_file, env_conf=env_conf, project_name=original_root.name)

        result = ''.join(output)

        # restore original root
        e2e_fixture._root = original_root

        assert code == 0
        assert 'ERROR' not in result
        assert 'WARNING' not in result
        assert '1 feature passed, 0 failed, 0 skipped' in result
        assert '3 scenarios passed, 0 failed, 0 skipped' in result
        assert 'steps passed, 0 failed, 0 skipped, 0 undefined' in result

        assert 'ident   iter  status   description' in result
        assert '001      2/2  passed   dog facts api' in result
        assert '002      1/1  passed   cat facts api' in result
        assert '003      1/1  passed   book api' in result
        assert '------|-----|--------|---------------|' in result

        assert 'executing custom.User.request for 002 get-cat-facts and /facts?limit=' in result

        assert 'sending "client_server" from CLIENT' in result
        assert "received from CLIENT" in result
        assert "AtomicCustomVariable.foobar='foobar'" in result

        # check debugging and that task index -> step expression is correct
        # dog facts api
        assert 'executing task 1 of 3: iterator' in result
        assert (
            'executing task 2 of 3: Then get request with name "get-dog-facts" from endpoint '
            '"/api/v1/resources/dogs?number={{ AtomicRandomInteger.dog_facts_count }}'
        ) in result
        assert 'executing task 3 of 3: pace' in result

        # cat facts api
        assert 'executing task 1 of 5: iterator' in result
        assert 'executing task 2 of 5: Then get request with name "get-cat-facts" from endpoint "/facts?limit={{ AtomicRandomInteger.cat_facts_count }}"' in result
        assert 'executing task 3 of 5: And send message "{\'client\': \'server\'}"' in result
        assert 'executing task 4 of 5: Then log message "foo={{ foo | touppercase }}, bar={{ bar | touppercase }}"' in result
        assert 'executing task 5 of 5: pace' in result

        assert 'foo=BAR, bar=BAR' in result

        # book api
        assert 'executing task 1 of 5: iterator' in result
        assert 'executing task 2 of 5: Then get request with name "1-get-book" from endpoint "/books/{{ AtomicCsvReader.books.book }}.json | content_type=json"' in result
        assert 'executing task 3 of 5: Then get request with name "2-get-author" from endpoint "{{ author_endpoint }}.json | content_type=json"' in result
        assert 'executing task 4 of 5: Then log message "AtomicCustomVariable.foobar=\'{{ steps.custom.AtomicCustomVariable.foobar }}\'"' in result
        assert 'executing task 5 of 5: pace' in result
    except:
        if result is not None:
            print(result)
        raise
