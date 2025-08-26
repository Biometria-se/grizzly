"""End-to-end test of example/."""

from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from shutil import copytree
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import End2EndFixture


def _prepare_example_project(e2e_fixture: End2EndFixture, root: Path) -> Path:
    # copy examples
    example_root = e2e_fixture.root.parent / 'test-example'
    copytree(root, example_root, dirs_exist_ok=True)

    with Path.joinpath(example_root, 'features', 'steps', 'steps.py').open('a') as fd:
        fd.write(
            e2e_fixture.step_start_webserver.format('/srv/grizzly'),
        )

    # create steps/webserver.py
    webserver_source = e2e_fixture.test_tmp_dir.parent / 'tests' / 'test_framework' / 'webserver.py'
    webserver_destination = example_root / 'features' / 'steps' / 'webserver.py'
    webserver_destination.write_text(webserver_source.read_text())

    feature_file_path = Path.joinpath(example_root, 'features', 'example.feature')
    feature_file_contents = feature_file_path.read_text().split('\n')

    index = feature_file_contents.index('  Scenario: dog facts api')
    # should go last in "Background"-section
    feature_file_contents.insert(index - 1, f'    Then start webserver on master port "{e2e_fixture.webserver.port}"')

    with Path(feature_file_path).open('w') as fd:
        fd.truncate(0)
        fd.write('\n'.join(feature_file_contents))

    return example_root


def _create_env_file_in_root(e2e_fixture: End2EndFixture) -> tuple[Path, dict[str, Any]]:
    root = (Path(__file__).parent / '..' / '..' / '..' / '..' / 'example').resolve()

    with Path.joinpath(root, 'environments', 'example.yaml').open() as env_yaml_file:
        env_conf = yaml.full_load(env_yaml_file)

        for name in ['dog', 'cat', 'book']:
            env_conf['configuration']['facts'][name]['host'] = f'http://{e2e_fixture.host}'

    return root, env_conf


def test_e2e_example(e2e_fixture: End2EndFixture) -> None:  # noqa: PLR0915
    result: str | None = None

    try:
        root, env_conf = _create_env_file_in_root(e2e_fixture)

        example_root = _prepare_example_project(e2e_fixture, root) if e2e_fixture._distributed else root

        feature_file = 'features/example.feature'

        # use test-example project stucture, but with original project name (image)
        original_root = e2e_fixture.root
        e2e_fixture._root = example_root

        try:
            code, output = e2e_fixture.execute(feature_file, env_conf=env_conf, project_name=original_root.name)

            result = ''.join(output)
        finally:
            # restore original root
            e2e_fixture._root = original_root

        assert code == 0

        assert 'Exception ignored in' not in result
        assert 'ERROR' not in result
        assert 'WARNING' not in result
        assert '1 feature passed, 0 failed, 0 skipped' in result
        assert '3 scenarios passed, 0 failed, 0 skipped' in result
        assert 'steps passed, 0 failed, 0 skipped' in result

        assert 'ident   iter  status   description' in result
        assert '001      2/2  passed   dog facts api' in result
        assert '002      1/1  passed   cat facts api' in result
        assert '003      1/1  passed   book api' in result
        assert '------|-----|--------|---------------|' in result

        assert 'executing custom.User.request for 002 get-cat-facts and /facts?limit=' in result

        assert 'sending "client_server" from CLIENT' in result
        assert 'received from CLIENT' in result
        assert "AtomicCustomVariable.foobar='foobar'" in result

        # check debugging and that task index -> step expression is correct
        # dog facts api
        assert '1 of 4 executed: iterator' in result
        assert '2 of 4 executed: Then get request with name "get-dog-facts" from endpoint "/api/v1/resources/dogs?number={{ AtomicRandomInteger.dog_facts_count }}' in result
        assert '3 of 4 executed: Then log message' in result
        assert '4 of 4 executed: pace' in result

        # cat facts api
        assert '1 of 6 executed: iterator' in result
        assert '2 of 6 executed: Then get request with name "get-cat-facts" from endpoint "/facts?limit={{ AtomicRandomInteger.cat_facts_count }}"' in result
        assert "3 of 6 executed: And send message \"{'client': 'server'}\"" in result
        assert '4 of 6 executed: Then log message "foo={{ foo | touppercase }}, bar={{ bar | touppercase }}"' in result
        assert '5 of 6 executed: Then log message' in result
        assert '6 of 6 executed: pace' in result

        assert 'foo=BAR, bar=BAR' in result

        # book api
        assert '1 of 6 executed: iterator' in result
        assert '2 of 6 executed: Then get request with name "1-get-book" from endpoint "/books/{{ AtomicCsvReader.books.book }}.json | content_type=json"' in result
        assert '3 of 6 executed: Then get request with name "2-get-author" from endpoint "{{ author_endpoint }}.json | content_type=json"' in result
        assert '4 of 6 executed: Then log message "AtomicCustomVariable.foobar=\'{{ steps.custom.AtomicCustomVariable.foobar }}\'"' in result
        assert '5 of 6 executed: Then log message' in result
        assert '6 of 6 executed: pace' in result

        # global var
        assert 'cat=foobar' in result
        assert 'dog=foobar' in result
        assert 'book=foobar' in result
    except:
        if result is not None:
            print(result)
        raise


def test_e2e_example_dry_run(e2e_fixture: End2EndFixture) -> None:
    result: str | None = None

    try:
        root, env_conf = _create_env_file_in_root(e2e_fixture)

        example_root = _prepare_example_project(e2e_fixture, root) if e2e_fixture._distributed else root

        feature_file = 'features/example.feature'

        # use test-example project stucture, but with original project name (image)
        original_root = e2e_fixture.root
        e2e_fixture._root = example_root

        try:
            code, output = e2e_fixture.execute(feature_file, env_conf=env_conf, project_name=original_root.name, dry_run=True)

            result = ''.join(output)
        finally:
            # restore original root
            e2e_fixture._root = original_root

        assert code == 0
        assert 'WARNING' not in result
        assert '1 feature passed, 0 failed, 0 skipped' in result
        assert '3 scenarios passed, 0 failed, 0 skipped' in result
        assert 'steps passed, 0 failed, 0 skipped' in result

        assert 'ident   iter  status   description' not in result

        assert 'executing custom.User.request for 002 get-cat-facts and /facts?limit=' not in result

        assert 'sending "client_server" from CLIENT' not in result
        assert 'received from CLIENT' not in result
        assert "AtomicCustomVariable.foobar='foobar'" not in result

        # check debugging and that task index -> step expression is correct
        # dog facts api
        assert 'executing task 1 of 4: iterator' not in result

        # cat facts api
        assert 'executing task 1 of 6: iterator' not in result

        assert 'foo=BAR, bar=BAR' not in result

        # book api
        assert 'executing task 1 of 6: iterator' not in result
    except:
        if result is not None:
            print(result)
        raise
    finally:
        with suppress(Exception):
            del e2e_fixture._env['GRIZZLY_DRY_RUN']
