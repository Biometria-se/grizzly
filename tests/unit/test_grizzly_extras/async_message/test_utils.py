import pytest

from grizzly_extras.async_message.utils import tohex


class Test_tohex:
    def test_unsupported(self) -> None:
        with pytest.raises(ValueError) as ve:
            tohex(['deadbeef'])  # type:ignore
        assert str(ve.value) == "['deadbeef'] has an unsupported type <class 'list'>"

    def test_int(self) -> None:
        assert tohex(3735928559) == 'deadbeef'

    def test_str(self) -> None:
        assert tohex('Þ­¾ï') == 'deadbeef'

    def test_bytes(self) -> None:
        assert tohex(b'\xde\xad\xbe\xef') == 'deadbeef'

    def test_bytearray(self) -> None:
        assert tohex(bytearray(b'\xde\xad\xbe\xef')) == 'deadbeef'
