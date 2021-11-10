from json import dumps as jsondumps

import pytest

from grizzly_extras.async_message import JsonBytesEncoder


class TestJsonBytesEncoder:
    def test_default(self) -> None:
        encoder = JsonBytesEncoder()

        assert encoder.default(b'hello') == 'hello'
        assert encoder.default(b'invalid \xe9 char') == 'invalid \xe9 char'

        assert jsondumps({
            'hello': b'world',
            'invalid': b'\xe9 char',
            'value': 'something',
            'test': False,
            'int': 1,
            'empty': None,
        }, cls=JsonBytesEncoder) == '{"hello": "world", "invalid": "\\u00e9 char", "value": "something", "test": false, "int": 1, "empty": null}'

        with pytest.raises(TypeError):
            encoder.default(None)

