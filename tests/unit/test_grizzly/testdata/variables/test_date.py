from typing import Literal
from datetime import datetime

import pytest
import gevent

from pytest_mock import MockerFixture
from dateutil.relativedelta import relativedelta

from grizzly.testdata.variables import AtomicDate
from grizzly.testdata.variables.date import atomicdate__base_type__

from tests.fixtures import AtomicVariableCleanupFixture


def test_atomicdate__base_type__() -> None:
    assert atomicdate__base_type__('now') == 'now'
    assert atomicdate__base_type__('now|format="%Y"') == 'now | format="%Y"'
    assert atomicdate__base_type__('now |format="%Y"') == 'now | format="%Y"'

    with pytest.raises(ValueError):
        atomicdate__base_type__('now| ')

    with pytest.raises(ValueError):
        atomicdate__base_type__('|')

    with pytest.raises(ValueError):
        atomicdate__base_type__('hello world')

    with pytest.raises(ValueError) as ve:
        atomicdate__base_type__('now | random=True')
    assert 'is not allowed' in str(ve)

    assert atomicdate__base_type__('2021-04-23T04:22:13.000Z') == '2021-04-23T04:22:13.000Z'
    assert atomicdate__base_type__('2021-04-26') == '2021-04-26'
    assert atomicdate__base_type__('1990-01-01 00:00:00') == '1990-01-01 00:00:00'

    with pytest.raises(ValueError) as ve:
        atomicdate__base_type__('now | timezone=NOT_A_VALID_TIMEZONE')
    assert 'date format is not specified' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicdate__base_type__('now | format="%Y", timezone=NOT_A_VALID_TIMEZONE')
    assert 'unknown timezone' in str(ve)


class TestAtomicDate:
    def test_now_value(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            with pytest.raises(ValueError):
                AtomicDate('now', 'asdf')

            t = AtomicDate('now', 'now')

            try:
                datetime.strptime(t['now'] or '', '%Y-%m-%d %H:%M:%S')
            except ValueError as e:
                pytest.fail(str(e))

            del t['now']

            t = AtomicDate('now', 'now|format="%Y-%m-%d %H:%M:%S.%f"')
            first = t['now']
            gevent.sleep(0.1)

            assert first != t['now']

            del t['now']

            t = AtomicDate('now', 'now | format="%Y-%m-%d %H:%M:%S.000Z"')

            with pytest.raises(ValueError):
                AtomicDate('now', 'now | format="%Y-%m-%d %H:%M:%S.000Z", calendar="gregorian"')
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_format(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            expected = datetime.now()

            t = AtomicDate('actual', 'now')

            with pytest.raises(NotImplementedError) as nie:
                t['actual'] = None
            assert str(nie.value) == 'AtomicDate has not implemented "__setitem__"'

            assert t['actual'] != expected.strftime('%Y-%m-%d %H:%M:%S.%f')

            del t['actual']
            del t['actual']

            with pytest.raises(AttributeError):
                # pylint: disable=pointless-statement
                t['actual']

            value = expected.strftime('%Y-%m-%d %H:%M:%S.%f')
            t = AtomicDate('actual', f'{value}|format="%Y-%m-%d"')

            assert t['actual'] is not None and expected.strftime('%Y-%m-%d %H:%M:%S.%f') != t['actual']
            del t['actual']

            t = AtomicDate('actual', f'{value}|format="%Y-%m-%d %H:%M:%S.%f"')
            assert t['actual'] is not None and expected.strftime('%Y-%m-%d %H:%M:%S.%f') == t['actual']
            del t['actual']

            with pytest.raises(ValueError):
                AtomicDate('actual', 'asdfasdf|format="%Y-%m-%d %H:%M:%S.%f"')

            t = AtomicDate('actual', 'now | format="%Y"')
            assert t['actual'] == datetime.now().strftime('%Y')
            del t['actual']

            t = AtomicDate('actual', "now | format='%Y-%m-%d %H'")
            assert t['actual'] == datetime.now().strftime('%Y-%m-%d %H')

            t = AtomicDate('test', 'now')
            assert len(t._settings.keys()) == 2
            assert 'test' in t._settings
            assert 'actual' in t._settings
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_timezone(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            expected_utc = datetime.utcnow().strftime('%H:%M')
            expected_local = datetime.now().strftime('%H:%M')

            t = AtomicDate('actual', 'now | format="%H:%M", timezone=UTC')
            assert t['actual'] == expected_utc
            assert t['actual'] != expected_local

            with pytest.raises(ValueError):
                AtomicDate('test', 'now | timezone=ASDF')
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_offset(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            expected = (datetime.now() + relativedelta(days=1)).strftime('%Y-%m-%d')

            t = AtomicDate('actual', 'now | format="%Y-%m-%d", offset=1D')
            assert t['actual'] == expected
            del t['actual']

            expected = (datetime.now() + relativedelta(years=-10, months=2, days=-2)).strftime('%Y-%m-%d')

            t = AtomicDate('actual', 'now | format="%Y-%m-%d", offset=-10Y2M-2D')
            assert t['actual'] == expected
            del t['actual']

            expected = '2017-10-12'
            t = AtomicDate('actual', '2017-10-26 | format="%Y-%m-%d", offset=-14D')
            assert t['actual'] == expected

            with pytest.raises(ValueError) as ve:
                AtomicDate('error', 'now | format="%Y", offset=10L')
            assert 'invalid time span format' in str(ve)
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_clear_and_destory(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            try:
                AtomicDate.destroy()
            except Exception:
                pass

            with pytest.raises(ValueError):
                AtomicDate.destroy()

            with pytest.raises(ValueError):
                AtomicDate.clear()

            expected = datetime.now()

            instance = AtomicDate('actual', 'now')

            assert instance['actual'] != expected.strftime('%Y-%m-%d %H:%M:%S.%f')

            assert len(instance._values.keys()) == 1
            assert len(instance._settings.keys()) == 1

            AtomicDate.clear()

            assert len(instance._values.keys()) == 0
            assert len(instance._settings.keys()) == 0
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test___getitem__error(self, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        def mocked__get_value(i: AtomicDate, variable: str) -> Literal[None]:
            return None

        mocker.patch(
            'grizzly.testdata.variables.date.AtomicDate._get_value',
            mocked__get_value,
        )

        try:
            t = AtomicDate('test', 'now | format="%Y-%m-%d"')

            with pytest.raises(ValueError) as ve:
                t['test']
            assert 'was incorrectly initialized with' in str(ve)
        finally:
            cleanup()
