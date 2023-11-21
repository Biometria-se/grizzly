"""Unit tests of grizzly_extras.async_message.utils."""
from __future__ import annotations

import pytest

from grizzly_extras.async_message.utils import tohex


class Test_tohex:
    def test_unsupported(self) -> None:
        with pytest.raises(ValueError, match='has an unsupported type'):
            tohex(['deadbeef'])

    def test_int(self) -> None:
        assert tohex(3735928559) == 'deadbeef'

    def test_str(self) -> None:
        assert tohex('Þ­¾ï') == 'deadbeef'

    def test_bytes(self) -> None:
        assert tohex(b'\xde\xad\xbe\xef') == 'deadbeef'

    def test_bytearray(self) -> None:
        assert tohex(bytearray(b'\xde\xad\xbe\xef')) == 'deadbeef'
