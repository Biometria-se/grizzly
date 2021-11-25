from typing import Any, Dict, List, Tuple, Optional, cast


def split_value(value: str, separator: str = '|') -> Tuple[str, str]:
    return cast(Tuple[str, str], tuple([v.strip() for v in value.split(separator, 1)]))


def get_unsupported_arguments(valid_arguments: List[str], arguments: Dict[str, Any]) -> List[str]:
    return [argument for argument in arguments.keys() if argument not in valid_arguments]


def parse_arguments(arguments: str, separator:str = '=') -> Dict[str, Any]:
    if separator not in arguments or (arguments.count(separator) > 1 and (arguments.count('"') < 2 and arguments.count("'") < 2) and ', ' not in arguments):
        raise ValueError(f'incorrect format in arguments: "{arguments}"')

    parsed: Dict[str, Any] = {}

    for argument in arguments.split(','):
        argument = argument.strip()

        if len(argument) < 1:
            raise ValueError(f'incorrect format for arguments: "{arguments}"')

        if separator not in argument:
            raise ValueError(f'incorrect format for argument: "{argument}"')

        [key, value] = argument.split(separator, 1)

        key = key.strip()
        if '"' in key or "'" in key or ' ' in key:
            raise ValueError('no quotes or spaces allowed in argument names')

        value = value.strip()

        if len(value) < 1:
            raise ValueError(f'invalid value for argument "{key}"')

        start_quote: Optional[str] = None

        if value[0] in ['"', "'"]:
            if value[-1] != value[0]:
                raise ValueError(f'value is incorrectly quoted: "{value}"')
            start_quote = value[0]
            value = value[1:]

        if value[-1] in ['"', "'"]:
            if start_quote is None:
                raise ValueError(f'value is incorrectly quoted: "{value}"')
            value = value[:-1]

        if start_quote is None and ' ' in value:
            raise ValueError(f'value needs to be quoted: "{value}"')

        parsed[key] = value

    return parsed