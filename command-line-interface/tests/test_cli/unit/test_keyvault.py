"""Tests for grizzly_cli.keyvault."""

from __future__ import annotations

from typing import TYPE_CHECKING

from grizzly_cli.keyvault import (
    COMMON_FALSE_POSITIVES,
    KEYWORDS,
    KeyvaultSecretHolder,
    _build_key_name,
    _determine_environment,
    _dict_to_yaml,
    _extract_metadata,
    _keyvault_normalize,
    _should_export,
    encode_file,
    encode_mq_certificate,
)

from test_cli.helpers import SOME, rm_rf

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock.plugin import MockerFixture


def test__keyvault_normalize() -> None:
    assert _keyvault_normalize('fo0b4R') == 'fo0b4R'
    assert _keyvault_normalize('hello.world!') == 'hello-world-'


def test_encode_mq_certificate(mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:
    glob_mock = mocker.patch('pathlib.Path.glob', return_value=None)
    root = tmp_path_factory.mktemp('test_context')

    try:
        # create "certificate" files
        kdb_file = root / 'foobar.kdb'
        kdb_file.write_bytes(b'A' * 25501)
        sth_file = root / 'foobar.sth'
        sth_file.write_bytes(b'B' * 255)

        glob_mock.return_value = iter([kdb_file, sth_file])

        encoded_mq_certificates = encode_mq_certificate(root, 'test', 'grizzly--test--mq-keyfile', 'foobar')
        assert encoded_mq_certificates == [
            SOME(KeyvaultSecretHolder, name='grizzly--test--foobar-kdb--0', content_type='file:foobar.kdb,chunk:0,chunks:2,noconf'),
            SOME(KeyvaultSecretHolder, name='grizzly--test--foobar-kdb--1', content_type='file:foobar.kdb,chunk:1,chunks:2,noconf'),
            SOME(KeyvaultSecretHolder, name='grizzly--test--foobar-kdb', content_type='files,noconf', value='grizzly--test--foobar-kdb--0,grizzly--test--foobar-kdb--1'),
            SOME(KeyvaultSecretHolder, name='grizzly--test--foobar-sth', content_type='file:foobar.sth,noconf'),
            SOME(KeyvaultSecretHolder, name='grizzly--test--mq-keyfile', content_type='files', value='grizzly--test--foobar-kdb,grizzly--test--foobar-sth'),
        ]
    finally:
        rm_rf(root)


def test_encode_file(tmp_path_factory: TempPathFactory) -> None:
    root = tmp_path_factory.mktemp('test_context')

    try:
        file = root / 'test.txt'
        file.write_bytes(b'C' * 512)

        keyvault_file = encode_file('grizzly--test--file', file.as_posix(), no_conf=False)
        assert keyvault_file == [
            SOME(KeyvaultSecretHolder, name='grizzly--test--file', content_type='file:test.txt'),
        ]
    finally:
        rm_rf(root)


def test__should_export() -> None:
    assert not _should_export('keyvault', 'test.vault.azure.net')
    assert not _should_export('hello.world', 'foobar')

    for keyword in KEYWORDS:
        assert _should_export(f'user.{keyword}', 'foobar')
        assert not _should_export(f'user.{keyword}.description', 'foobar')
        assert _should_export('user.description', f'hello{keyword.upper()}')

        for common_false_positive in COMMON_FALSE_POSITIVES:
            assert not _should_export('user.description', common_false_positive)


def test__determine_environment() -> None:
    assert _determine_environment([], 'test', 'foo.bar') == 'test'
    assert _determine_environment(['foo'], 'test', 'foo.bar') == 'global'


def test__build_key_name() -> None:
    assert _build_key_name('test', 'foo.bar') == 'grizzly--test--foo-bar'
    assert _build_key_name('global', 'hello.world.foo.bar') == 'grizzly--global--hello-world-foo-bar'


def test__dict_to_yaml(tmp_path_factory: TempPathFactory) -> None:
    root = tmp_path_factory.mktemp('test_context')

    try:
        file = root / 'test.yaml'
        file.write_text('lorem ipsum')

        content: dict = {
            'foo': {
                'bar': 'hello world',
            },
            'hello': 'world',
            'test': {
                'struct': {
                    'with': [
                        'value1',
                        'value2',
                    ],
                },
            },
        }

        _dict_to_yaml(file, content, indentation=8)

        assert (
            file.read_text()
            == """foo:
        bar: hello world
hello: world
test:
        struct:
                with:
                        - value1
                        - value2
"""
        )
        _dict_to_yaml(file, content, indentation=2)

        assert (
            file.read_text()
            == """foo:
  bar: hello world
hello: world
test:
  struct:
    with:
      - value1
      - value2
"""
        )
    finally:
        rm_rf(root)


def test__extract_metadata(tmp_path_factory: TempPathFactory) -> None:
    root = tmp_path_factory.mktemp('test_context')

    try:
        env_file = root / 'test.yaml'
        env = {
            'configuration': {
                'keyvault': 'https://test.vault.azure.net',
                'env': 'test',
                'foo': {
                    'bar': 'hello world',
                },
            },
        }

        _dict_to_yaml(env_file, env, indentation=2)

        assert _extract_metadata(env_file.as_posix()) == (
            'test',
            'https://test.vault.azure.net',
            {
                'keyvault': 'https://test.vault.azure.net',
                'env': 'test',
                'foo.bar': 'hello world',
            },
        )

        env = {
            'configuration': {
                'foo': {
                    'bar': 'hello world',
                },
            },
        }

        _dict_to_yaml(env_file, env, indentation=2)

        assert _extract_metadata(env_file.as_posix()) == (
            'test',
            None,
            {
                'foo.bar': 'hello world',
            },
        )
    finally:
        rm_rf(root)
