"""Unit tests for grizzly_common.transformer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from json import dumps as jsondumps
from json import loads as jsonloads
from json.decoder import JSONDecodeError
from typing import TYPE_CHECKING, Any

import pytest
from grizzly_common.transformer import (
    JsonBytesEncoder,
    JsonTransformer,
    PlainTransformer,
    Transformer,
    TransformerContentType,
    XmlTransformer,
    transformer,
)
from lxml import etree as XML  # noqa: N812

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from pytest_mock import MockerFixture


JSON_EXAMPLE = {
    'glossary': {
        'title': 'example glossary',
        'GlossDiv': {
            'title': 'S',
            'GlossList': {
                'GlossEntry': {
                    'ID': 'SGML',
                    'SortAs': 'SGML',
                    'GlossTerm': 'Standard Generalized Markup Language',
                    'Acronym': 'SGML',
                    'Abbrev': 'ISO 8879:1986',
                    'GlossDef': {
                        'para': 'A meta-markup language, used to create markup languages such as DocBook.',
                        'GlossSeeAlso': ['GML', 'XML'],
                    },
                    'GlossSee': 'markup',
                    'Additional': [
                        {
                            'addtitle': 'test1',
                            'addvalue': 'hello world',
                        },
                        {
                            'addtitle': 'test2',
                            'addvalue': 'good stuff',
                        },
                    ],
                },
            },
        },
    },
}


class TestTransformer:
    def test_abstract_class(self) -> None:
        class DummyTransformer(Transformer):
            @classmethod
            def transform(cls, raw: str) -> Any:
                return super().transform(raw)

            @classmethod
            def validate(cls, expression: str) -> bool:
                return super().validate(expression)

            @classmethod
            def parser(cls, expression: str) -> Callable[[Any], list[str]]:
                return super().parser(expression)

        with pytest.raises(NotImplementedError, match='has not implemented transform'):
            DummyTransformer.transform('{}')

        with pytest.raises(NotImplementedError, match='has not implemented validate'):
            DummyTransformer.validate('')

        with pytest.raises(NotImplementedError, match='has not implemented parse'):
            DummyTransformer.parser('')


class Testtransformer:
    def test___init__(self) -> None:
        transformers: list[transformer] = []

        assert TransformerContentType.get_vector() == (False, True)

        for content_type in TransformerContentType:
            if content_type == TransformerContentType.UNDEFINED:
                continue

            t = transformer(content_type)
            assert t.content_type == content_type
            transformers.append(t)

        with pytest.raises(ValueError, match='it is not allowed to register a transformer of type UNDEFINED'):
            transformer(TransformerContentType.UNDEFINED)

        for index, current in enumerate(transformers, start=1):
            previous = transformers[index - 1]
            assert current.available is previous.available

    def test___call__(self, mocker: MockerFixture) -> None:
        class DummyTransformer(Transformer):
            @classmethod
            def transform(cls, _raw: str) -> Any:
                return None

            @classmethod
            def validate(cls, _expression: str) -> bool:
                return True

            @classmethod
            def parser(cls, expression: str) -> Callable[[Any], list[str]]:
                return super().parser(expression)

        transform_spy = mocker.spy(DummyTransformer, 'transform')

        # restore some method metadata
        DummyTransformer.transform.__name__ = 'transform'
        DummyTransformer.transform.__qualname__ = 'DummyTransformer.transform'
        DummyTransformer.transform.__annotations__ = {'_raw': str, 'return': Any}

        transform_spy.side_effect = [None, None, None, (TransformerContentType.JSON, {'test': 'value'}), (TransformerContentType.UNDEFINED, {'test': 'value'})]

        original_transformers = transformer.available.copy()

        try:
            assert len(original_transformers) == 3
            assert len(transformer.available) == 3
            transformer_decorator = transformer(TransformerContentType.JSON)

            wrapped = transformer_decorator(DummyTransformer)

            assert len(transformer.available) == 3

            payload_json = '{"test": "value"}'

            wrapped.transform(payload_json)
            assert transform_spy.call_count == 1

        finally:
            # remove dummy transformer
            transformer.available = original_transformers
            assert len(transformer.available) == 3


class TestJsonTransformer:
    def test_transform(self) -> None:
        unwrapped = JsonTransformer.__wrapped_transform__
        assert unwrapped('{}') == {}

        assert JsonTransformer.transform('{}') == {}

        with pytest.raises(JSONDecodeError, match='Expecting property name enclosed in double quotes'):
            unwrapped('{')

    def test__get_outer_op(self) -> None:
        assert JsonTransformer._get_outer_op('$.`this`[?hello=="world"]', '==') is None
        assert JsonTransformer._get_outer_op('$.`this`[?hello=="world"].id>=2000', '>=') == ('$.`this`[?hello=="world"].id', '2000')
        assert JsonTransformer._get_outer_op('$.`this`[?hello=="world"].id>=2000', '==') is None
        assert JsonTransformer._get_outer_op('$.`this`[?hello=="world" & version>=2].timestamp>="2025-01-13T13:38:27.000000Z"', '>=') == (
            '$.`this`[?hello=="world" & version>=2].timestamp',
            '"2025-01-13T13:38:27.000000Z"',
        )
        assert JsonTransformer._get_outer_op('$.glossary.title=="example glossary"', '==') == ('$.glossary.title', '"example glossary"')
        assert JsonTransformer._get_outer_op('$.id|=[2, 3]', '|=') == ('$.id', '[2, 3]')
        assert JsonTransformer._get_outer_op('$.`this`[?id>1].id|=[2, 3]', '|=') == ('$.`this`[?id>1].id', '[2, 3]')
        assert JsonTransformer._get_outer_op("$.result[0].recipients[?(@.uid=='1000')]", '==') is None
        assert JsonTransformer._get_outer_op("$.result[0].recipients[?(@.uid=='1000')].username=='hello'", '==') == (
            "$.result[0].recipients[?(@.uid=='1000')].username",
            "'hello'",
        )
        assert JsonTransformer._get_outer_op('$.evil.edgecase=="]"', '==') == ('$.evil.edgecase', '"]"')
        assert JsonTransformer._get_outer_op('$.evil.edgecase=="["', '==') == ('$.evil.edgecase', '"["')
        assert JsonTransformer._get_outer_op('$.evil.edgecase==[]', '==') == ('$.evil.edgecase', '[]')

    def test_validate(self) -> None:
        assert JsonTransformer.validate('$.test.something')
        assert JsonTransformer.validate('$.`this`[?status="ready"]')
        assert not JsonTransformer.validate('$.')
        assert not JsonTransformer.validate('.test.something')

    def test_parser(self) -> None:  # noqa: PLR0915
        get_values = JsonTransformer.parser('$.test.value')
        assert callable(get_values)

        actual = get_values({'test': {'value': 'test'}})
        assert len(actual) == 1
        assert actual == ['test']

        get_values = JsonTransformer.parser('$.test[*].value')
        assert callable(get_values)
        actual = get_values({'test': [{'value': 'test1'}, {'value': 'test2'}]})

        assert len(actual) == 2
        assert actual == ['test1', 'test2']

        with pytest.raises(ValueError, match=r'JsonTransformer: unable to parse with ".*": not a valid expression'):
            JsonTransformer.parser('$......asdf')

        with pytest.raises(ValueError, match=r'JsonTransformer: unable to parse with ".*": not a valid expression'):
            JsonTransformer.parser('test')

        with pytest.raises(ValueError, match=r'JsonTransformer: unable to parse with ".*": not a valid expression'):
            JsonTransformer.parser('$.')

        get_values = JsonTransformer.parser('$.glossary.GlossDiv.GlossList.GlossEntry.Abbrev')
        actual = get_values(JSON_EXAMPLE)
        assert len(actual) == 1
        assert actual == ['ISO 8879:1986']

        get_values = JsonTransformer.parser('$.glossary.title=="example glossary"')
        actual = get_values(JSON_EXAMPLE)
        assert actual == ['example glossary']

        get_values = JsonTransformer.parser("$.glossary.title=='template glossary'")
        actual = get_values(JSON_EXAMPLE)
        assert actual == []

        get_values = JsonTransformer.parser('$.*..title')
        actual = get_values(JSON_EXAMPLE)
        assert len(actual) == 2
        assert actual == ['example glossary', 'S']

        get_values = JsonTransformer.parser('$.*..GlossSeeAlso')
        actual = get_values(JSON_EXAMPLE)
        assert len(actual) == 1
        assert actual == ['["GML", "XML"]']
        assert jsonloads(actual[0]) == ['GML', 'XML']

        get_values = JsonTransformer.parser('$.*..GlossSeeAlso[*]')
        actual = get_values(JSON_EXAMPLE)
        assert len(actual) == 2
        assert actual == ['GML', 'XML']

        get_values = JsonTransformer.parser('$..Additional[?addtitle="test1"].addvalue')
        actual = get_values(JSON_EXAMPLE)
        assert len(actual) == 1
        assert actual == ['hello world']

        get_values = JsonTransformer.parser('$.`this`')
        actual = get_values(False)  # noqa: FBT003
        assert len(actual) == 1
        assert actual == ['False']

        example = {
            'document': {
                'name': 'test',
                'id': 13,
            },
        }
        get_values = JsonTransformer.parser('$.`this`[?(@.name="test")]')
        actual = get_values(example)

        assert len(actual) > 0

        document = {
            'name': 'foobar',
            'id': 1,
            'description': 'foo bar',
        }

        get_values = JsonTransformer.parser('$.`this`[?name="foobar" & id=1]')
        actual = get_values(document)

        assert actual != []

        # <!-- test equal
        get_values = JsonTransformer.parser('$.id==2')
        actual = get_values(document)
        assert actual == []

        get_values = JsonTransformer.parser('$.id==1')
        actual = get_values(document)
        assert actual == ['1']

        get_values = JsonTransformer.parser('$.`this`[?id==1 & name=="foobar"].id')
        actual = get_values(document)
        assert actual == ['1']
        # // -->

        # <!-- test or
        get_values = JsonTransformer.parser('$.id|=[2, 3]')
        actual = get_values(document)
        assert actual == []

        get_values = JsonTransformer.parser("$.id|='[1, 2, 3]'")
        actual = get_values(document)
        assert actual == ['1']

        document.update({'id': 3})
        actual = get_values(document)
        assert actual == ['3']

        document.update({'id': 2})
        actual = get_values(document)
        assert actual == ['2']

        get_values = JsonTransformer.parser('$.id|="[1, 2, 3]"')
        document.update({'id': 1})
        actual = get_values(document)
        assert actual == ['1']

        document.update({'id': 3})
        actual = get_values(document)
        assert actual == ['3']

        document.update({'id': 2})
        actual = get_values(document)
        assert actual == ['2']
        # // -->

        # <!-- greater than or equals, datetime
        now = datetime.now(tz=timezone.utc)
        actual_timestamp = now.isoformat().replace('+00:00', 'Z')
        message = f"""{{
  "externalId": "2fc91fbb-091a-4132-8d00-c84d2c8dd85b",
  "internalId": "GRZ2FC91FBB091A4132",
  "timestamp": "{actual_timestamp}",
  "version": 2
}}"""

        expected_timestamp = (now - timedelta(minutes=3)).isoformat().replace('+00:00', 'Z')

        parser = JsonTransformer.parser(f'$.`this`[?version==2].timestamp>="{expected_timestamp}"')
        assert parser(JsonTransformer.transform(message)) == [actual_timestamp]
        # // -->


class TestXmlTransformer:
    def test_transform(self) -> None:
        unwrapped = XmlTransformer.__wrapped_transform__
        transformed = unwrapped(
            """<?xml version="1.0" encoding="UTF-8"?>
            <test>
                value
            </test>""",
        )

        assert isinstance(transformed, XML._Element)

        with pytest.raises(XML.ParseError, match='Namespace prefix test on test is not defined'):
            unwrapped(
                """<?xml version="1.0" encoding="UTF-8"?>
                <test:test>
                    value
                </test:test>""",
            )

        with pytest.raises(XML.XMLSyntaxError, match='Start tag expected'):
            unwrapped(
                '{"test": "value"}',
            )

    def test_validate(self) -> None:
        assert XmlTransformer.validate('/test/something')
        assert XmlTransformer.validate('//*[@id="root"]/section')
        assert XmlTransformer.validate('/test/something/child::*')

        assert not XmlTransformer.validate('/hello/')
        assert not XmlTransformer.validate('/[id="root"]/section')
        assert not XmlTransformer.validate('//*[[@id="react-root"]/section')

    def test_parser(self) -> None:
        with pytest.raises(ValueError, match=r'XmlTransformer: unable to parse ".*": invalid expression'):
            XmlTransformer.parser('/hello/')

        input_payload = XmlTransformer.transform(
            """<?xml version="1.0" encoding="UTF-8"?>
            <test>
                value
            </test>""",
        )
        get_values = XmlTransformer.parser('/test/text()')
        assert callable(get_values)
        actual = get_values(input_payload)
        assert len(actual) == 1
        assert actual[0] == 'value'

        input_payload = XmlTransformer.transform(
            """<?xml version="1.0" encoding="UTF-8"?>
            <parent>
                <child id="1337">
                    some text
                </child>
                <child id="1338">
                    some other text
                </child>
            </parent>""",
        )
        actual = get_values(input_payload)
        assert len(actual) == 0

        get_values = XmlTransformer.parser('//child/text()')
        actual = get_values(input_payload)
        assert len(actual) == 2
        assert actual == ['some text', 'some other text']

        # example is a stripped down version of https://data.cityofnewyork.us/api/views/825b-niea/rows.xml?accessType=DOWNLOAD
        example = """<?xml version="1.0" encoding="UTF-8"?>
        <response>
            <row>
                <row
                    _id="row-yvru.xsvq_qzbq"
                    _uuid="00000000-0000-0000-1B32-87B29F69422E"
                    _position="0"
                    _address="https://data.cityofnewyork.us/resource/_825b-niea/row-yvru.xsvq_qzbq"
                >
                    <grade>3</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9768</number_tested>
                    <mean_scale_score>700</mean_scale_score>
                    <level_1_1>243</level_1_1>
                </row>
                <row
                    _id="row-q8z8.q7b3.3ppa"
                    _uuid="00000000-0000-0000-D9CE-B1F89A0D1307"
                    _position="0"
                    _address="https://data.cityofnewyork.us/resource/_825b-niea/row-q8z8.q7b3.3ppa"
                >
                    <grade>4</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9973</number_tested>
                    <mean_scale_score>699</mean_scale_score>
                    <level_1_1>294</level_1_1>
                </row>
                <row
                    _id="row-i23x-4prc-46fj"
                    _uuid="00000000-0000-0000-C9EE-2418870B5F93"
                    _position="0"
                    _address="https://data.cityofnewyork.us/resource/_825b-niea/row-i23x-4prc-46fj"
                >
                    <grade>5</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9852</number_tested>
                    <mean_scale_score>691</mean_scale_score>
                    <level_1_1>369</level_1_1>
                </row>
                <row
                    _id="row-7u9v-dwwy.fhw3"
                    _uuid="00000000-0000-0000-17FD-7D50A499A0E1"
                    _position="0"
                    _address="https://data.cityofnewyork.us/resource/_825b-niea/row-7u9v-dwwy.fhw3"
                >
                    <grade>6</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9606</number_tested>
                    <mean_scale_score>682</mean_scale_score>
                    <level_1_1>452</level_1_1>
                </row>
                <row
                    _id="row-64kf_k4ma_4zgq"
                    _uuid="00000000-0000-0000-6A3C-917EFD40527E"
                    _position="0"
                    _address="https://data.cityofnewyork.us/resource/_825b-niea/row-64kf_k4ma_4zgq"
                >
                    <grade>7</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9433</number_tested>
                    <mean_scale_score>671</mean_scale_score>
                    <level_1_1>521</level_1_1>
                </row>
                <row
                    _id="row-h8zg-qxyq.g4ge"
                    _uuid="00000000-0000-0000-07BB-C9FC65F5ADEE"
                    _position="0"
                    _address="https://data.cityofnewyork.us/resource/_825b-niea/row-h8zg-qxyq.g4ge"
                >
                    <grade>8</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9593</number_tested>
                    <mean_scale_score>675</mean_scale_score>
                    <level_1_1>671</level_1_1>
                </row>
                <row
                    _id="row-2e2j.nmuz_pgdd"
                    _uuid="00000000-0000-0000-BEEE-72B03BE38AC8"
                    _position="0"
                    _address="https://data.cityofnewyork.us/resource/_825b-niea/row-2e2j.nmuz_pgdd"
                >
                    <grade>All Grades</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>58225</number_tested>
                    <mean_scale_score>687</mean_scale_score>
                    <level_1_1>2550</level_1_1>
                </row>
            </row>
        </response>"""

        input_payload = XmlTransformer.transform(example)

        get_values = XmlTransformer.parser('//row/@_id')
        actual = get_values(input_payload)
        assert len(actual) == 7

        get_values = XmlTransformer.parser('//row/level_1_1[. > 500]/text()')
        actual = get_values(input_payload)
        assert actual == ['521', '671', '2550']

        example = """<?xml version="1.0" encoding="UTF-8"?>
        <documents>
            <document>
                <header>
                    <id>DOCUMENT_1337-3</id>
                    <type>application/docx</type>
                    <author>Douglas Adams</author>
                    <published>2021-11-01</published>
                    <pages>241</pages>
                </header>
            </document>
        </documents>"""

        input_payload = XmlTransformer.transform(example)

        get_values = XmlTransformer.parser('/documents/document/header')
        actual = get_values(input_payload)
        assert actual == ['<header><id>DOCUMENT_1337-3</id><type>application/docx</type><author>Douglas Adams</author><published>2021-11-01</published><pages>241</pages></header>']

        example = """<?xml version="1.0" encoding="utf-8"?>
<root xmlns:foo="http://www.foo.org/" xmlns:bar="http://www.bar.org">
  <actors>
    <actor id="7">Christian Bale</actor>
    <actor id="8">Liam Neeson</actor>
    <actor id="9">Michael Caine</actor>
  </actors>
  <foo:singers>
    <foo:singer id="10">Tom Waits</foo:singer>
    <foo:singer id="11">B.B. King</foo:singer>
    <foo:singer id="12">Ray Charles</foo:singer>
  </foo:singers>
</root>"""

        input_payload = XmlTransformer.transform(example)

        get_values = XmlTransformer.parser('/root/actors/actor[@id="9"]')
        actual = get_values(input_payload)
        assert actual == ['<actor id="9">Michael Caine</actor>']

        get_values = XmlTransformer.parser('/root/actors/child::*')
        actual = get_values(input_payload)
        assert actual == [
            '<actor id="7">Christian Bale</actor>',
            '<actor id="8">Liam Neeson</actor>',
            '<actor id="9">Michael Caine</actor>',
        ]


class TestPlainTransformer:
    def test_transform(self) -> None:
        unwrapped = PlainTransformer.__wrapped_transform__

        transformed = unwrapped('plain text')

        assert isinstance(transformed, str)

    def test_validate(self) -> None:
        assert PlainTransformer.validate('asdf')
        assert PlainTransformer.validate('.*hello (\\w+)$')

    def test_parser(self) -> None:
        match_value = PlainTransformer.parser('asdf')

        assert callable(match_value)

        actual = match_value('asdf')
        assert len(actual) == 1
        assert actual[0] == 'asdf'

        with pytest.raises(ValueError, match='PlainTransformer: only expressions that has zero or one match group is allowed'):
            PlainTransformer.parser('///(test1)(test2)///')

        match_value = PlainTransformer.parser('///(test1)///')
        actual = match_value('///test1///')
        assert len(actual) == 1
        assert actual[0] == 'test1'

        match_value = PlainTransformer.parser('.*used to.*')
        actual = match_value('A meta-markup language, used to create markup languages such as DocBook.')
        assert len(actual) == 1
        assert actual[0] == 'A meta-markup language, used to create markup languages such as DocBook.'

        match_value = PlainTransformer.parser('.*(used to).*')
        actual = match_value('A meta-markup language, used to create markup languages such as DocBook.')
        assert len(actual) == 1
        assert actual[0] == 'used to'

        match_value = PlainTransformer.parser('.*(used to).*')
        assert match_value('test test test test') == []

        match_value = PlainTransformer.parser('[)hello')
        actual = match_value('[)hello')
        assert len(actual) == 1
        assert actual[0] == '[)hello'
        assert match_value('hello') == []


class TestJsonBytesEncoder:
    def test_default(self) -> None:
        encoder = JsonBytesEncoder()

        assert encoder.default(b'hello') == 'hello'
        assert encoder.default(b'invalid \xe9 char') == 'invalid \xe9 char'

        assert (
            jsondumps(
                {
                    'hello': b'world',
                    'invalid': b'\xe9 char',
                    'value': 'something',
                    'test': False,
                    'int': 1,
                    'empty': None,
                },
                cls=JsonBytesEncoder,
            )
            == '{"hello": "world", "invalid": "\\u00e9 char", "value": "something", "test": false, "int": 1, "empty": null}'
        )

        with pytest.raises(TypeError, match='is not JSON serializable'):
            encoder.default(None)


class TestTransformerContentType:
    def test_json(self) -> None:
        for value in ['json', 'JSON', 'application/json']:
            assert TransformerContentType.from_string(value) == TransformerContentType.JSON

    def test_xml(self) -> None:
        for value in ['xml', 'XML', 'application/xml']:
            assert TransformerContentType.from_string(value) == TransformerContentType.XML

    def test_plain(self) -> None:
        for value in ['PLAIN', 'plain', 'text/plain']:
            assert TransformerContentType.from_string(value) == TransformerContentType.PLAIN

    def test_undefined(self) -> None:
        for value in ['undefined', 'UNDEFINED']:
            assert TransformerContentType.from_string(value) == TransformerContentType.UNDEFINED

    def test_unknown(self) -> None:
        with pytest.raises(ValueError, match='"foo" is an unknown response content type'):
            TransformerContentType.from_string('foo')
