from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from grizzly_ls.server.inventory import (
    _filter_source_directories,
    compile_inventory,
    create_step_normalizer,
)

from test_ls.conftest import GRIZZLY_PROJECT
from test_ls.helpers import (
    DummyEnum,
    DummyEnumNoFromString,
    DummyEnumNoFromStringType,
    parse_enum_indirect,
    parse_with_pattern,
    parse_with_pattern_and_vector,
    parse_with_pattern_error,
)

if TYPE_CHECKING:
    from _pytest.logging import LogCaptureFixture
    from pytest_mock import MockerFixture

    from test_ls.fixtures import LspFixture


def test_create_normalizer(lsp_fixture: LspFixture, mocker: MockerFixture) -> None:
    ls = lsp_fixture.server
    namespace = 'grizzly_ls.server.inventory'

    mocker.patch(
        f'{namespace}.ParseMatcher.TYPE_REGISTRY',
        {},
    )

    normalizer = create_step_normalizer(ls)
    assert normalizer.custom_types == {}

    mocker.patch(
        f'{namespace}.ParseMatcher.TYPE_REGISTRY',
        {
            'WithPatternAndVector': parse_with_pattern_and_vector,
            'WithPattern': parse_with_pattern,
            'EnumIndirect': parse_enum_indirect,
            'EnumDirect': DummyEnum.from_string,
        },
    )
    normalizer = create_step_normalizer(ls)

    assert sorted(normalizer.custom_types.keys()) == sorted(
        [
            'WithPatternAndVector',
            'WithPattern',
            'EnumIndirect',
            'EnumDirect',
        ]
    )

    with_pattern_and_vector = normalizer.custom_types.get('WithPatternAndVector', None)

    assert with_pattern_and_vector is not None
    assert not with_pattern_and_vector.permutations.x
    assert with_pattern_and_vector.permutations.y
    assert sorted(with_pattern_and_vector.replacements) == sorted(['bar', 'hello', 'foo', 'world'])

    with_pattern = normalizer.custom_types.get('WithPattern', None)

    assert with_pattern is not None
    assert not with_pattern.permutations.x
    assert not with_pattern.permutations.y
    assert sorted(with_pattern.replacements) == sorted(['alice', 'bob'])

    enum_indirect = normalizer.custom_types.get('EnumIndirect', None)
    assert enum_indirect is not None
    assert enum_indirect.permutations.x
    assert enum_indirect.permutations.y
    assert sorted(enum_indirect.replacements) == sorted(['client_server', 'server_client'])

    enum_direct = normalizer.custom_types.get('EnumDirect', None)
    assert enum_direct is not None
    assert not enum_direct.permutations.x
    assert not enum_direct.permutations.y
    assert sorted(enum_direct.replacements) == sorted(['hello', 'world', 'foo', 'bar'])

    mocker.patch(
        f'{namespace}.ParseMatcher.TYPE_REGISTRY',
        {
            'WithPattern': parse_with_pattern_error,
        },
    )

    with pytest.raises(ValueError, match=r'could not extract pattern from "@parse.with_pattern\(\'\'\)" for custom type WithPattern'):
        create_step_normalizer(ls)

    mocker.patch(
        f'{namespace}.ParseMatcher.TYPE_REGISTRY',
        {
            'EnumError': DummyEnumNoFromString.magic,
        },
    )

    create_step_normalizer(ls)

    mocker.patch(
        f'{namespace}.ParseMatcher.TYPE_REGISTRY',
        {
            'EnumError': DummyEnumNoFromStringType.from_string,
        },
    )

    with pytest.raises(ValueError, match='could not find the type that from_string method for custom type EnumError returns'):
        create_step_normalizer(ls)


def test__filter_source_paths(mocker: MockerFixture) -> None:
    m = mocker.patch('pathlib.Path.is_dir', return_value=True)

    base_path = Path('my', 'directory')

    test_paths = [
        Path.joinpath(base_path, '.venv', 'foo', 'file.py'),
        Path.joinpath(base_path, 'node_modules', 'sub1', 'sub2', 'sub.py'),
        Path.joinpath(base_path, 'bin', 'some_bin.py'),
        Path.joinpath(base_path, 'steps', 'step1.py'),
        Path.joinpath(base_path, 'steps', 'step2.py'),
        Path.joinpath(base_path, 'steps', 'helpers', 'helper.py'),
        Path.joinpath(base_path, 'util', 'utils.py'),
        Path.joinpath(base_path, 'util', 'utils2.py'),
        Path.joinpath(base_path, 'util', 'utils2.py'),
    ]

    # Default, subdirectories under .venv and node_modules should be ignored,
    # and bin directory
    file_ignore_patterns: list[str] = []
    filtered = _filter_source_directories(file_ignore_patterns, test_paths)
    assert m.call_count == 9
    assert len(filtered) == 3
    assert Path.joinpath(base_path, 'steps') in filtered
    assert Path.joinpath(base_path, 'steps', 'helpers') in filtered
    assert Path.joinpath(base_path, 'util') in filtered

    # Ignore util directory
    file_ignore_patterns = ['**/util']
    filtered = _filter_source_directories(file_ignore_patterns, test_paths)
    assert len(filtered) == 5
    assert Path.joinpath(base_path, '.venv', 'foo') in filtered
    assert Path.joinpath(base_path, 'node_modules', 'sub1', 'sub2') in filtered
    assert Path.joinpath(base_path, 'steps') in filtered
    assert Path.joinpath(base_path, 'steps', 'helpers') in filtered
    assert Path.joinpath(base_path, 'bin') in filtered

    # Ignore steps and any subdirectory under it
    file_ignore_patterns = ['**/steps']
    filtered = _filter_source_directories(file_ignore_patterns, test_paths)
    assert len(filtered) == 4
    assert Path.joinpath(base_path, '.venv', 'foo') in filtered
    assert Path.joinpath(base_path, 'node_modules', 'sub1', 'sub2') in filtered
    assert Path.joinpath(base_path, 'util') in filtered
    assert Path.joinpath(base_path, 'bin') in filtered


def test_compile_inventory(lsp_fixture: LspFixture, caplog: LogCaptureFixture) -> None:
    ls = lsp_fixture.server

    ls.steps.clear()

    assert ls.steps == {}

    ls.root_path = GRIZZLY_PROJECT

    with caplog.at_level(logging.INFO, 'GrizzlyLanguageServer'):
        compile_inventory(ls)

    assert len(caplog.messages) == 2

    assert ls.steps != {}
    assert len(ls.normalizer.custom_types.keys()) >= 8

    keywords = list(ls.steps.keys())

    for keyword in ['given', 'then', 'when']:
        assert keyword in keywords


def test_compile_keyword_inventory(lsp_fixture: LspFixture) -> None:
    ls = lsp_fixture.server

    ls.steps.clear()

    assert ls.steps == {}

    # create pre-requisites
    ls.root_path = GRIZZLY_PROJECT

    # indirect call to `compile_keyword_inventory`
    compile_inventory(ls)

    assert 'Feature' not in ls.keywords  # already used once in feature file
    assert 'Background' not in ls.keywords  # - " -
    assert 'And' in ls.keywords  # just an alias for Given, but we need want it
    assert 'Scenario' in ls.keywords  # can be used multiple times
    assert 'Given' in ls.keywords  # - " -
    assert 'When' in ls.keywords
