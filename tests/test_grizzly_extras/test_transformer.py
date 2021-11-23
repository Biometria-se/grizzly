from typing import List, Tuple, Any
from json import dumps as jsondumps, loads as jsonloads
from json.decoder import JSONDecodeError

import pytest

from lxml import etree as XML
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from grizzly_extras.transformer import (
    JsonTransformer,
    Transformer,
    XmlTransformer,
    PlainTransformer,
    transformer,
    JsonBytesEncoder,
    TransformerError,
    TransformerContentType,
)


class TestTransformer:
    def test_abstract_class(self) -> None:
        class DummyTransformer(Transformer):
            pass

        with pytest.raises(NotImplementedError):
            DummyTransformer.transform(TransformerContentType.JSON, '{}')

        with pytest.raises(NotImplementedError):
            DummyTransformer.validate('')

        with pytest.raises(NotImplementedError):
            DummyTransformer.parser('')


class Testtransformer:
    def test___init__(self) -> None:
        transformers: List[transformer] = []

        for content_type in TransformerContentType:
            if content_type == TransformerContentType.GUESS:
                continue

            t = transformer(content_type)
            assert t.content_type == content_type
            transformers.append(t)

        with pytest.raises(ValueError) as ve:
            transformer(TransformerContentType.GUESS)
        assert 'it is not allowed to register a transformer of type GUESS' in str(ve)

        for index, current in enumerate(transformers, start=1):
            previous = transformers[index-1]
            assert current.available is previous.available

    def test___call__(self, mocker: MockerFixture) -> None:
        class DummyTransformer(Transformer):
            @classmethod
            def transform(cls, content_type: TransformerContentType, raw: str) -> Tuple[TransformerContentType, Any]:
                pass

            @classmethod
            def validate(cls, expression: str) -> bool:
                pass

        transform_spy = mocker.spy(DummyTransformer, 'transform')
        transform_spy.side_effect = [None, None, None, (TransformerContentType.JSON, {'test': 'value'}), (TransformerContentType.GUESS, {'test': 'value'})]

        original_transformers = transformer.available.copy()

        try:
            assert len(transformer.available) == 3
            transformer_decorator = transformer(TransformerContentType.JSON)

            wrapped = transformer_decorator(DummyTransformer)

            assert len(transformer.available) == 3

            payload_xml = '<?xml version="1.0" encoding="UTF-8"?><test>value</test>'
            payload_json = '{"test": "value"}'
            payload_json_error = '{"test: "value"}'

            assert wrapped.transform(TransformerContentType.XML, payload_xml) == (TransformerContentType.GUESS, payload_xml)
            assert transform_spy.call_count == 0

            assert wrapped.transform(TransformerContentType.GUESS, payload_xml) == (TransformerContentType.GUESS, payload_xml)
            assert transform_spy.call_count == 1

            with pytest.raises(TransformerError) as te:
                wrapped.transform(TransformerContentType.JSON, payload_xml)
            assert 'failed to transform input as JSON' in str(te)
            assert transform_spy.call_count == 2

            with pytest.raises(TransformerError) as te:
                wrapped.transform(TransformerContentType.JSON, payload_json_error)
            assert 'failed to transform input as JSON' in str(te)
            assert transform_spy.call_count == 3

            wrapped.transform(TransformerContentType.JSON, payload_json)
            assert transform_spy.call_count == 4

            assert wrapped.transform(TransformerContentType.GUESS, payload_json) == (TransformerContentType.JSON, {'test': 'value'})
            assert transform_spy.call_count == 5
        finally:
            # remove dummy transformer
            transformer.available = original_transformers


class TestJsonTransformer:
    def test_transform(self) -> None:
        unwrapped = JsonTransformer.__wrapped_transform__  # type: ignore
        assert unwrapped(TransformerContentType.GUESS, '{}') == (TransformerContentType.GUESS, {})
        assert unwrapped(TransformerContentType.JSON, '{}') == (TransformerContentType.JSON, {})

        # this will never happen when decorated with transformer
        assert unwrapped(TransformerContentType.XML, '{}') == (TransformerContentType.XML, {})
        assert JsonTransformer.transform(TransformerContentType.XML, '{}') == (TransformerContentType.GUESS, '{}')

        with pytest.raises(JSONDecodeError):
            unwrapped(TransformerContentType.GUESS, '{')

    def test_validate(self) -> None:
        assert JsonTransformer.validate('$.test.something')
        assert not JsonTransformer.validate('$.')
        assert not JsonTransformer.validate('.test.something')

    def test_parser(self) -> None:
        get_values = JsonTransformer.parser('$.test.value')
        assert callable(get_values)

        actual = get_values({'test': {'value': 'test'}})
        assert len(actual) == 1
        assert actual == ['test']

        get_values = JsonTransformer.parser('$.test[*].value')
        assert callable(get_values)
        actual = get_values({'test': [{'value': 'test1'}, {'value': 'test2'}]})

        assert len(actual) == 2
        assert ['test1', 'test2'] == actual

        with pytest.raises(ValueError):
            JsonTransformer.parser('$......asdf')

        with pytest.raises(ValueError):
            JsonTransformer.parser('test')

        with pytest.raises(ValueError):
            JsonTransformer.parser('$.')

        example = {
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
                                'GlossSeeAlso': ['GML', 'XML']
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
                            ]
                        }
                    }
                }
            }
        }

        get_values = JsonTransformer.parser('$.glossary.GlossDiv.GlossList.GlossEntry.Abbrev')
        actual = get_values(example)
        assert len(actual) == 1
        assert ['ISO 8879:1986'] == actual

        get_values = JsonTransformer.parser('$.*..title')
        actual = get_values(example)
        assert len(actual) == 2
        assert ['example glossary', 'S'] == actual

        get_values = JsonTransformer.parser('$.*..GlossSeeAlso')
        actual = get_values(example)
        assert len(actual) == 1
        assert ['["GML", "XML"]'] == actual
        assert jsonloads(actual[0]) == ['GML', 'XML']

        get_values = JsonTransformer.parser('$.*..GlossSeeAlso[*]')
        actual = get_values(example)
        assert len(actual) == 2
        assert ['GML', 'XML'] == actual

        get_values = JsonTransformer.parser('$..Additional[?addtitle="test1"].addvalue')
        actual = get_values(example)
        assert len(actual) == 1
        assert actual == ['hello world']

        get_values = JsonTransformer.parser('$.`this`')
        actual = get_values(False)
        assert len(actual) == 1
        assert actual == ['False']


class TestXmlTransformer:
    def test_transform(self) -> None:
        unwrapped = XmlTransformer.__wrapped_transform__  # type: ignore
        content_type, transformed = unwrapped(
            TransformerContentType.GUESS,
            '''<?xml version="1.0" encoding="UTF-8"?>
            <test>
                value
            </test>''',
        )

        assert content_type == TransformerContentType.GUESS
        print(type(transformed))
        assert isinstance(transformed, XML._Element)

        content_type, transformed = unwrapped(
            TransformerContentType.XML,
            '''<?xml version="1.0" encoding="UTF-8"?>
            <test>
                value
            </test>''',
        )

        assert content_type == TransformerContentType.XML
        assert isinstance(transformed, XML._Element)

        # this will never happen when decorated with transformer
        content_type, transformed = unwrapped(
            TransformerContentType.JSON,
            '''<?xml version="1.0" encoding="UTF-8"?>
            <test>
                value
            </test>''',
        )

        assert content_type == TransformerContentType.JSON
        assert isinstance(transformed, XML._Element)

        assert XmlTransformer.transform(
            TransformerContentType.JSON,
            '''<?xml version="1.0" encoding="UTF-8"?>
            <test>
                value
            </test>''',
        ) == (
            TransformerContentType.GUESS,
            '''<?xml version="1.0" encoding="UTF-8"?>
            <test>
                value
            </test>''',
        )

        with pytest.raises(XML.ParseError):
            unwrapped(
                TransformerContentType.XML,
                '''<?xml version="1.0" encoding="UTF-8"?>
                <test:test>
                    value
                </test:test>''',
            )

        with pytest.raises(XML.XMLSyntaxError):
            unwrapped(
                TransformerContentType.XML,
                '{"test": "value"}',
            )

    def test_validate(self) -> None:
        assert XmlTransformer.validate('/test/something')
        assert XmlTransformer.validate('//*[@id="root"]/section')

        assert not XmlTransformer.validate('/hello/')
        assert not XmlTransformer.validate('/[id="root"]/section')
        assert not XmlTransformer.validate('//*[[@id="react-root"]/section')

    def test_parser(self) -> None:
        with pytest.raises(ValueError):
            XmlTransformer.parser('/hello/')

        _, input_payload = XmlTransformer.transform(
            TransformerContentType.XML,
            '''<?xml version="1.0" encoding="UTF-8"?>
            <test>
                value
            </test>''',
        )
        get_values = XmlTransformer.parser('/test/text()')
        assert callable(get_values)
        actual = get_values(input_payload)
        assert len(actual) == 1
        assert actual[0] == 'value'

        _, input_payload = XmlTransformer.transform(
            TransformerContentType.XML,
            '''<?xml version="1.0" encoding="UTF-8"?>
            <parent>
                <child id="1337">
                    some text
                </child>
                <child id="1338">
                    some other text
                </child>
            </parent>''',
        )
        actual = get_values(input_payload)
        assert len(actual) == 0

        get_values = XmlTransformer.parser('//child/text()')
        actual = get_values(input_payload)
        assert len(actual) == 2
        assert actual == ['some text', 'some other text']

        # example is a stripped down version of https://data.cityofnewyork.us/api/views/825b-niea/rows.xml?accessType=DOWNLOAD
        example = '''<?xml version="1.0" encoding="UTF-8"?>
        <response>
            <row>
                <row _id="row-yvru.xsvq_qzbq" _uuid="00000000-0000-0000-1B32-87B29F69422E" _position="0" _address="https://data.cityofnewyork.us/resource/_825b-niea/row-yvru.xsvq_qzbq">
                    <grade>3</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9768</number_tested>
                    <mean_scale_score>700</mean_scale_score>
                    <level_1_1>243</level_1_1>
                </row>
                <row _id="row-q8z8.q7b3.3ppa" _uuid="00000000-0000-0000-D9CE-B1F89A0D1307" _position="0" _address="https://data.cityofnewyork.us/resource/_825b-niea/row-q8z8.q7b3.3ppa">
                    <grade>4</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9973</number_tested>
                    <mean_scale_score>699</mean_scale_score>
                    <level_1_1>294</level_1_1>
                </row>
                <row _id="row-i23x-4prc-46fj" _uuid="00000000-0000-0000-C9EE-2418870B5F93" _position="0" _address="https://data.cityofnewyork.us/resource/_825b-niea/row-i23x-4prc-46fj">
                    <grade>5</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9852</number_tested>
                    <mean_scale_score>691</mean_scale_score>
                    <level_1_1>369</level_1_1>
                </row>
                <row _id="row-7u9v-dwwy.fhw3" _uuid="00000000-0000-0000-17FD-7D50A499A0E1" _position="0" _address="https://data.cityofnewyork.us/resource/_825b-niea/row-7u9v-dwwy.fhw3">
                    <grade>6</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9606</number_tested>
                    <mean_scale_score>682</mean_scale_score>
                    <level_1_1>452</level_1_1>
                </row>
                <row _id="row-64kf_k4ma_4zgq" _uuid="00000000-0000-0000-6A3C-917EFD40527E" _position="0" _address="https://data.cityofnewyork.us/resource/_825b-niea/row-64kf_k4ma_4zgq">
                    <grade>7</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9433</number_tested>
                    <mean_scale_score>671</mean_scale_score>
                    <level_1_1>521</level_1_1>
                </row>
                <row _id="row-h8zg-qxyq.g4ge" _uuid="00000000-0000-0000-07BB-C9FC65F5ADEE" _position="0" _address="https://data.cityofnewyork.us/resource/_825b-niea/row-h8zg-qxyq.g4ge">
                    <grade>8</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>9593</number_tested>
                    <mean_scale_score>675</mean_scale_score>
                    <level_1_1>671</level_1_1>
                </row>
                <row _id="row-2e2j.nmuz_pgdd" _uuid="00000000-0000-0000-BEEE-72B03BE38AC8" _position="0" _address="https://data.cityofnewyork.us/resource/_825b-niea/row-2e2j.nmuz_pgdd">
                    <grade>All Grades</grade>
                    <year>2006</year>
                    <category>Asian</category>
                    <number_tested>58225</number_tested>
                    <mean_scale_score>687</mean_scale_score>
                    <level_1_1>2550</level_1_1>
                </row>
            </row>
        </response>'''

        _, input_payload = XmlTransformer.transform(TransformerContentType.XML, example)

        get_values = XmlTransformer.parser('//row/@_id')
        actual = get_values(input_payload)
        assert len(actual) == 7

        get_values = XmlTransformer.parser('//row/level_1_1[. > 500]/text()')
        actual = get_values(input_payload)
        assert actual == ['521', '671', '2550']

        example = '''<?xml version="1.0" encoding="UTF-8"?>
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
        </documents>'''

        _, input_payload = XmlTransformer.transform(TransformerContentType.XML, example)

        get_values = XmlTransformer.parser('/documents/document/header')
        actual = get_values(input_payload)
        assert actual == ['<header><id>DOCUMENT_1337-3</id><type>application/docx</type><author>Douglas Adams</author><published>2021-11-01</published><pages>241</pages></header>']

        example = '''<?xml version="1.0" encoding="utf-8"?>
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
</root>'''

        _, input_payload = XmlTransformer.transform(TransformerContentType.XML, example)

        get_values = XmlTransformer.parser('/root/actors/actor[@id="9"]')
        actual = get_values(input_payload)
        assert actual == ['<actor id="9">Michael Caine</actor>']


class TestPlainTransformer:
    def test_transform(self) -> None:
        unwrapped = PlainTransformer.__wrapped_transform__  # type: ignore

        with pytest.raises(NotImplementedError):
            unwrapped(TransformerContentType.PLAIN, 'plain text')

    def test_validate(self) -> None:
        assert PlainTransformer.validate('asdf')
        assert PlainTransformer.validate('.*hello (\\w+)$')

    def test_parser(self) -> None:
        match_value = PlainTransformer.parser('asdf')

        assert callable(match_value)

        actual = match_value('asdf')
        assert len(actual) == 1
        assert actual[0] == 'asdf'

        with pytest.raises(ValueError):
            PlainTransformer.parser('///(test1)(test2)///')

        match_value = PlainTransformer.parser('///(test1)///')
        actual = match_value('///test1///')
        assert len(actual) == 1
        assert 'test1' == actual[0]

        match_value = PlainTransformer.parser('.*used to.*')
        actual = match_value('A meta-markup language, used to create markup languages such as DocBook.')
        assert len(actual) == 1
        assert 'A meta-markup language, used to create markup languages such as DocBook.' == actual[0]

        match_value = PlainTransformer.parser('.*(used to).*')
        actual = match_value('A meta-markup language, used to create markup languages such as DocBook.')
        assert len(actual) == 1
        assert 'used to' == actual[0]

        match_value = PlainTransformer.parser('.*(used to).*')
        assert match_value('test test test test') == []

        match_value = PlainTransformer.parser('[)hello')
        actual = match_value('[)hello')
        assert len(actual) == 1
        assert '[)hello' == actual[0]
        assert match_value('hello') == []


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
