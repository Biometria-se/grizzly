import pytest

from grizzly_extras.arguments import split_value, get_unsupported_arguments, parse_arguments

@pytest.mark.parametrize('separator', ['|', ', '])
def test_split_value(separator: str) -> None:
    assert split_value(f'hello world  {separator} foo bar', separator) == ('hello world', 'foo bar',)
    assert split_value(
        f'hello {separator} world {separator} foo {separator} bar', separator
    ) == (
        'hello', f'world {separator} foo {separator} bar',
    )


def test_get_unsupported_arguments() -> None:
    assert get_unsupported_arguments(['hello', 'world'], {
        'hello': True,
        'world': True,
        'foo': True,
        'bar': False,
    }) == ['foo', 'bar']

    assert get_unsupported_arguments(['hello', 'world', 'foo', 'bar'], {
        'hello': True,
        'world': True,
        'foo': True,
        'bar': False,
    }) == []


@pytest.mark.parametrize('separator', ['=', ':', '%'])
def test_parse_arguments(separator: str) -> None:
    with pytest.raises(ValueError) as ve:
        parse_arguments('argument', separator)
    assert 'incorrect format in arguments:' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'arg1{separator}test arg2{separator}value', separator)
    assert 'incorrect format in arguments:' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'args1{separator}test,', separator)
    assert 'incorrect format for arguments:' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'args1{separator}test,arg2', separator)
    assert 'incorrect format for argument:' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'"test"{separator}value', separator)
    assert 'no quotes or spaces allowed in argument names' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f"'test'{separator}value", separator)
    assert 'no quotes or spaces allowed in argument names' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'test variable{separator}value', separator)
    assert 'no quotes or spaces allowed in argument names' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'arg{separator}"value\'', separator)
    assert 'value is incorrectly quoted' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f"arg{separator}'value\"", separator)
    assert 'value is incorrectly quoted' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'arg{separator}value"', separator)
    assert 'value is incorrectly quoted' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f"arg{separator}value'", separator)
    assert 'value is incorrectly quoted' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'arg{separator}test value', separator)
    assert 'value needs to be quoted' in str(ve)

    arguments = parse_arguments(f'arg1{separator}testvalue1, arg2{separator}"test value 2"', separator)

    assert arguments == {
        'arg1': 'testvalue1',
        'arg2': 'test value 2',
    }

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'url{separator}http://www.example.com?query_string{separator}value', separator)
    assert 'incorrect format in arguments: ' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f'url{separator}"http://www.example.com?query_string{separator}value', separator)
    assert 'incorrect format in arguments: ' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_arguments(f"url{separator}'http://www.example.com?query_string{separator}value", separator)
    assert 'incorrect format in arguments: ' in str(ve)

    arguments = parse_arguments(f"url{separator}'http://www.example.com?query_string{separator}value', argument{separator}False", separator)
    assert arguments == {
        'url': f'http://www.example.com?query_string{separator}value',
        'argument': 'False',
    }
