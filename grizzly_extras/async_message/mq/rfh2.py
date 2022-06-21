import gzip
import time
import xml.etree.ElementTree as ET
from struct import pack, unpack
from typing import List, Optional

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

# Basic MQ RFH2 support with gzip compression
# See https://www.ibm.com/docs/en/ibm-mq/9.1?topic=mqi-mqrfh2-rules-formatting-header-2

RFH2_STRUC_ID = 'RFH '
RFH2_VERSION = 2
RFH2_HEADER_LENGTH = 36


class Rfh2Decoder():
    @classmethod
    def is_rfh2(cls, message: bytes) -> bool:
        try:
            parts = unpack('<4c l', message[0:8])
            struc_id = b''.join(list(parts[0:4])).decode('ascii')
            version = parts[4]
            return struc_id == RFH2_STRUC_ID and version == RFH2_VERSION
        except:
            return False

    def __init__(self, message: bytes) -> None:
        self._parse_header(message)
        self._parse_name_values()

    def _parse_header(self, message: bytes) -> None:
        try:
            parts = unpack('<4c l l l l 8c l l', message[0:RFH2_HEADER_LENGTH])
            # Only "struc_length" is needed/used right now, the rest is
            # kept commented out, for any future need :)
            # struc_id = b''.join(list(parts[0:4])).decode('ascii')
            # version = parts[4]
            struc_length = parts[5]
            # encoding = parts[6]
            # charset = parts[7]
            # fmt = b''.join(list(parts[8:16])).decode('ascii')
            # flags = parts[16]
            # name_value_ccsid = parts[17]
            self.name_values = message[RFH2_HEADER_LENGTH:struc_length]
            self.payload = message[RFH2_HEADER_LENGTH + len(self.name_values):]
        except Exception as e:
            raise ValueError('Failed to parse RFH2 header', e)

    def _parse_name_values(self) -> None:
        self.name_value_parts: List[ET.Element] = []
        pos = 0
        maxpos = len(self.name_values) - 1
        if maxpos == -1:
            return
        try:
            while pos <= maxpos:
                part_length = unpack('<l', self.name_values[pos:pos + 4])[0]
                pos += 4
                part = self.name_values[pos:pos + part_length].decode('utf-8')
                self.name_value_parts.append(ET.fromstring(part))
                pos += part_length
        except Exception as e:
            raise ValueError('Failed to parse RFH2 name values', e)

    def _get_usr_encoding(self) -> Optional[str]:
        if len(self.name_value_parts) == 0:
            return None
        for part in self.name_value_parts:
            if part.tag == 'usr':
                encoding_element = part.find('ContentEncoding')
                if encoding_element is None:
                    return None
                return encoding_element.text
        return None

    def get_payload(self) -> bytes:
        content_encoding = self._get_usr_encoding()
        if content_encoding == 'gzip':
            return gzip.decompress(self.payload)
        return self.payload


class Rfh2Encoder():
    CCSID = 1208
    ENCODING = 546
    PADDING_MULTIPLE = 4
    FMT_NONE = ' ' * 8
    FLAGS = 0

    @classmethod
    def create_md(cls) -> pymqi.MD:
        md = pymqi.MD()
        md.Format = 'MQHRF2  '.encode()
        md.CodedCharSetId = Rfh2Encoder.CCSID
        md.Encoding = Rfh2Encoder.ENCODING
        return md

    def __init__(self, payload: bytes, queue_name: str, encoding: str = 'gzip', tstamp: Optional[str] = None) -> None:
        if encoding != 'gzip':
            raise NotImplementedError('Only gzip encoding is implemented')
        self.queue_name = queue_name
        self.content_encoding = encoding
        self.tstamp = tstamp
        self.message = bytearray()

        self._build_payload(payload)
        self._build_name_values()
        self._build_header()

    def _build_payload(self, payload: bytes) -> None:
        self.payload = gzip.compress(payload)

    def _build_name_values(self) -> None:
        padding_multiple = Rfh2Encoder.PADDING_MULTIPLE
        self.name_values = bytearray()
        content_length = len(self.payload)
        if self.tstamp is None:
            tstamp = str(round(time.time() * 1000))
        else:
            tstamp = self.tstamp
        name_values_txt = [
            "<mcd><Msd>jms_bytes</Msd></mcd>",
            f"<jms><Dst>queue:///{self.queue_name}</Dst><Tms>{tstamp}</Tms><Dlv>2</Dlv></jms>",
            f"<usr><ContentEncoding>{self.content_encoding}</ContentEncoding><ContentLength dt='i8'>{content_length}</ContentLength></usr>",
        ]
        for value in name_values_txt:
            value_len = len(value)

            # pad with spaces to get length to a multiple of 4
            if value_len % padding_multiple != 0:
                padding_length = padding_multiple - value_len % padding_multiple
                value = value + ' ' * padding_length

            name_value = value.encode()
            name_value_length = len(name_value)
            self.name_values.extend(pack('<l', name_value_length))
            self.name_values.extend(name_value)

    def _build_header(self) -> None:
        struc_length = RFH2_HEADER_LENGTH + len(self.name_values)
        encoding = Rfh2Encoder.ENCODING
        charset = Rfh2Encoder.CCSID
        fmt = Rfh2Encoder.FMT_NONE
        flags = Rfh2Encoder.FLAGS
        name_value_ccsid = Rfh2Encoder.CCSID
        self.header = pack('<4s l l l l 8s l l',
                           RFH2_STRUC_ID.encode(),
                           RFH2_VERSION,
                           struc_length,
                           encoding,
                           charset,
                           fmt.encode(),
                           flags,
                           name_value_ccsid)

    def get_message(self) -> bytes:
        message = bytearray()
        message.extend(self.header)
        message.extend(self.name_values)
        message.extend(self.payload)
        return bytes(message)
