import time
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta
from typing import Optional

import pytest

from pytest_mock.plugin import MockerFixture

from grizzly_extras.async_message.mq import Rfh2Decoder, Rfh2Encoder

rfh2_msg = b'RFH \x02\x00\x00\x00\xfc\x00\x00\x00"\x02\x00\x00\xb8\x04\x00\x00        \x00\x00\x00\x00\xb8\x04\x00\x00 \x00\x00\x00<mcd><Msd>jms_bytes</Msd></mcd> ' \
           b'P\x00\x00\x00<jms><Dst>queue:///TEST.QUEUE</Dst><Tms>1655406556138</Tms><Dlv>2</Dlv></jms>   \\\x00\x00\x00<usr><ContentEncoding>gzip</ContentEncoding>' \
           b'<ContentLength dt=\'i8\'>32</ContentLength></usr> \x1f\x8b\x08\x00\xdc\x7f\xabb\x02\xff+I-.Q(H\xac\xcc\xc9OL\x01\x00\xe1=\x1d\xeb\x0c\x00\x00\x00'
# with header
rfh2_msg2 = b'RFH \x02\x00\x00\x00\x1c\x01\x00\x00"\x02\x00\x00\xb8\x04\x00\x00        \x00\x00\x00\x00\xb8\x04\x00\x00 \x00\x00\x00<mcd><Msd>jms_bytes</Msd></mcd> ' \
            b'P\x00\x00\x00<jms><Dst>queue:///OTHERQUEUE</Dst><Tms>1234567890123</Tms><Dlv>2</Dlv></jms>   |\x00\x00\x00<usr><some_name>some_value</some_name>' \
            b'<ContentEncoding>gzip</ContentEncoding><ContentLength dt=\'i8\'>32</ContentLength></usr>' \
            b'\x1f\x8b\x08\x00\xbb54c\x02\xff+I-.Q(H\xac\xcc\xc9OL\x01\x00\xe1=\x1d\xeb\x0c\x00\x00\x00'


class TestRfh2Decoder:
    def test_is_rfh2(self) -> None:
        assert Rfh2Decoder.is_rfh2(rfh2_msg) is True
        assert Rfh2Decoder.is_rfh2(b'how you doin') is False
        assert Rfh2Decoder.is_rfh2(b'\x00') is False

    def test__parse_header(self, mocker: MockerFixture) -> None:
        parse_header_spy = mocker.spy(Rfh2Decoder, '_parse_header')
        d = Rfh2Decoder(rfh2_msg)
        assert parse_header_spy.call_count == 1
        assert d.name_values == rfh2_msg[36:252]
        assert d.payload == rfh2_msg[252:]

        with pytest.raises(ValueError) as ve:
            Rfh2Decoder(rfh2_msg[0:10])
        assert 'Failed to parse RFH2 header' in str(ve)

    def test__parse_name_values(self) -> None:
        # test normal flow
        d = Rfh2Decoder(rfh2_msg)
        assert len(d.name_value_parts) == 3
        assert ET.tostring(d.name_value_parts[0], encoding='unicode', method='xml').endswith('<mcd><Msd>jms_bytes</Msd></mcd>')
        assert ET.tostring(d.name_value_parts[1], encoding='unicode', method='xml').endswith('<jms><Dst>queue:///TEST.QUEUE</Dst><Tms>1655406556138</Tms><Dlv>2</Dlv></jms>')
        assert ET.tostring(d.name_value_parts[2], encoding='unicode', method='xml')\
            .endswith('<usr><ContentEncoding>gzip</ContentEncoding><ContentLength dt="i8">32</ContentLength></usr>')

        # test maxpos == -1
        d.name_values = b''
        d._parse_name_values()
        assert len(d.name_value_parts) == 0

        # test too short name values
        d = Rfh2Decoder(rfh2_msg)
        d.name_values = d.name_values[0:4]
        with pytest.raises(ValueError) as ve:
            d._parse_name_values()
        assert 'Failed to parse RFH2 name values' in str(ve)

        # test invalid name value tags
        d = Rfh2Decoder(rfh2_msg)
        d.name_values = b' \x00\x00\x00<mcd><Msd>jms_bytes</Msd></mcd> ' \
                        b'P\x00\x00\x00<jms><Dst>queue:///TEST.QUEUE</Dst><Tms>1655406556138</Tms><Dlv>2</Dlv></jXX>   \\\x00\x00\x00<usr><ContentEncoding>gzip</ContentEncoding>' \
                        b'<ContentLength dt=\'i8\'>32</ContentLength></usr> \x1f\x8b\x08\x00\xdc\x7f\xabb\x02\xff+I-.Q(H\xac\xcc\xc9OL\x01\x00\xe1=\x1d\xeb\x0c\x00\x00\x00'
        with pytest.raises(ValueError) as ve:
            d._parse_name_values()
        assert 'Failed to parse RFH2 name values' in str(ve)

    def test__get_usr_encoding(self) -> None:
        # test normal flow
        d = Rfh2Decoder(rfh2_msg)
        assert d._get_usr_encoding() == 'gzip'

        # test no name value parts
        d.name_value_parts = []
        assert d._get_usr_encoding() is None

        # test no encoding element
        no_enc = b'RFH \x02\x00\x00\x00\xfc\x00\x00\x00"\x02\x00\x00\xb8\x04\x00\x00        \x00\x00\x00\x00\xb8\x04\x00\x00 \x00\x00\x00<mcd><Msd>jms_bytes</Msd></mcd> ' \
                 b'P\x00\x00\x00<jms><Dst>queue:///TEST.QUEUE</Dst><Tms>1655406556138</Tms><Dlv>2</Dlv></jms>   \\\x00\x00\x00<usr><ZontentEncoding>gzip</ZontentEncoding>' \
                 b'<ContentLength dt=\'i8\'>32</ContentLength></usr> \x1f\x8b\x08\x00\xdc\x7f\xabb\x02\xff+I-.Q(H\xac\xcc\xc9OL\x01\x00\xe1=\x1d\xeb\x0c\x00\x00\x00'
        d = Rfh2Decoder(no_enc)
        assert d._get_usr_encoding() is None

        # test no usr element
        no_enc = b'RFH \x02\x00\x00\x00\xfc\x00\x00\x00"\x02\x00\x00\xb8\x04\x00\x00        \x00\x00\x00\x00\xb8\x04\x00\x00 \x00\x00\x00<mcd><Msd>jms_bytes</Msd></mcd> ' \
                 b'P\x00\x00\x00<jms><Dst>queue:///TEST.QUEUE</Dst><Tms>1655406556138</Tms><Dlv>2</Dlv></jms>   \\\x00\x00\x00<uzr><ContentEncoding>gzip</ContentEncoding>' \
                 b'<ContentLength dt=\'i8\'>32</ContentLength></uzr> \x1f\x8b\x08\x00\xdc\x7f\xabb\x02\xff+I-.Q(H\xac\xcc\xc9OL\x01\x00\xe1=\x1d\xeb\x0c\x00\x00\x00'
        d = Rfh2Decoder(no_enc)
        assert d._get_usr_encoding() is None

    def test_get_payload(self, mocker: MockerFixture) -> None:
        # test normal flow
        d = Rfh2Decoder(rfh2_msg)
        payload = d.get_payload().decode('utf-8')
        assert payload == 'test payload'

        # test with non-gzip encoding
        def mocked_get_usr_encoding(p: Rfh2Decoder) -> Optional[str]:
            return 'vulcan'

        mocker.patch.object(
            Rfh2Decoder,
            '_get_usr_encoding',
            mocked_get_usr_encoding,
        )
        assert d.get_payload() == rfh2_msg[252:]


class TestRfh2Encoder:
    def test___init__(self) -> None:
        # test normal flow
        Rfh2Encoder('test payload'.encode(), queue_name='DUMMYQ')

        # test invalid encoding
        with pytest.raises(NotImplementedError) as nie:
            Rfh2Encoder('test payload'.encode(), queue_name='DUMMYQ', encoding='wrong')
        assert 'Only gzip encoding is implemented' in str(nie)

    def test__build_payload(self) -> None:
        e = Rfh2Encoder('test payload'.encode(), queue_name='TEST.QUEUE', tstamp='1655406556138')
        # gzip seem to vary, just compare the first bytes
        assert e.payload[0:4] == rfh2_msg[252:256]

    def test__build_name_values(self) -> None:
        # test normal flow
        e = Rfh2Encoder('test payload'.encode(), queue_name='TEST.QUEUE', tstamp='1655406556138')
        assert e.name_values == rfh2_msg[36:252]

        # test encoding other queue name and timestamp
        e = Rfh2Encoder('test payload'.encode(), queue_name='OTHERQUEUE', tstamp='1234567890123')
        assert e.name_values[59:69] == b'OTHERQUEUE'
        assert e.name_values[80:93] == b'1234567890123'

        # test (force) encoding other content encoding than gzip
        e.content_encoding = 'blah'
        e._build_name_values()
        assert e.name_values[146:150] == b'blah'

        # test generating timestamp
        tstamp_before = str(round(time.time() * 1000))
        datetime_before = datetime.fromtimestamp(int(tstamp_before) / 1000)
        e = Rfh2Encoder('test payload'.encode(), queue_name='OTHERQUEUE')
        generated_tstamp = e.name_values[80:93].decode('utf-8')
        generated_datetime = datetime.fromtimestamp(int(generated_tstamp) / 1000)
        assert generated_datetime >= datetime_before
        assert generated_datetime - timedelta(hours=1) < datetime_before

        # test metadata/headers
        e = Rfh2Encoder('test payload'.encode(), queue_name='OTHERQUEUE', tstamp='1234567890123', metadata={'some_name': 'some_value'})
        assert e.name_values == rfh2_msg2[36:284]

    def test__build_header(self) -> None:
        e = Rfh2Encoder('test payload'.encode(), queue_name='TEST.QUEUE', tstamp='1655406556138')
        assert e.header == rfh2_msg[0:36]

    def test_get_message(self) -> None:
        e = Rfh2Encoder('test payload'.encode(), queue_name='TEST.QUEUE', tstamp='1655406556138')
        msg = e.get_message()
        # check up until gzip compression data
        assert msg[0:252] == rfh2_msg[0:252]

    def test_encode_decode(self) -> None:
        src_payload = 'test payload'
        e = Rfh2Encoder(src_payload.encode(), queue_name='TEST.QUEUE', tstamp='1655406556138')
        msg = e.get_message()
        d = Rfh2Decoder(msg)
        assert d.get_payload().decode() == src_payload

    def test_encode_decode_metadata(self) -> None:
        src_payload = 'test payload'
        e = Rfh2Encoder(src_payload.encode(), queue_name='TEST.QUEUE', tstamp='1655406556138', metadata={'some_key': 'some_value'})
        msg = e.get_message()
        d = Rfh2Decoder(msg)
        assert d.get_payload().decode() == src_payload
