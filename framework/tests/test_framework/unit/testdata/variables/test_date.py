"""Unit tests of grizzly.testdata.variables.date."""

from __future__ import annotations

from contextlib import suppress
from datetime import datetime
from typing import TYPE_CHECKING

import gevent
import pytest
from dateutil.relativedelta import relativedelta
from grizzly.testdata.variables import AtomicDate
from grizzly.testdata.variables.date import atomicdate__base_type__
from grizzly.types import ZoneInfo

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture, MockerFixture


def test_atomicdate__base_type__() -> None:
    assert atomicdate__base_type__('now') == 'now'
    assert atomicdate__base_type__('now|format="%Y"') == 'now | format="%Y"'
    assert atomicdate__base_type__('now |format="%Y"') == 'now | format="%Y"'

    with pytest.raises(ValueError, match='incorrect format in arguments: ""'):
        atomicdate__base_type__('now| ')

    with pytest.raises(ValueError, match='incorrect format in arguments: ""'):
        atomicdate__base_type__('|')

    with pytest.raises(ValueError, match='Unknown string format: hello world'):
        atomicdate__base_type__('hello world')

    with pytest.raises(ValueError, match='is not allowed'):
        atomicdate__base_type__('now | random=True')

    assert atomicdate__base_type__('2021-04-23T04:22:13.000Z') == '2021-04-23T04:22:13.000Z'
    assert atomicdate__base_type__('2021-04-26') == '2021-04-26'
    assert atomicdate__base_type__('1990-01-01 00:00:00') == '1990-01-01 00:00:00'

    with pytest.raises(ValueError, match='date format is not specified'):
        atomicdate__base_type__('now | timezone=NOT_A_VALID_TIMEZONE')

    with pytest.raises(ValueError, match='unknown timezone'):
        atomicdate__base_type__('now | format="%Y", timezone=NOT_A_VALID_TIMEZONE')


class TestAtomicDate:
    def test_now_value(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario

        try:
            with pytest.raises(ValueError, match='Unknown string format: asdf'):
                AtomicDate(scenario=scenario1, variable='now', value='asdf')

            t = AtomicDate(scenario=scenario1, variable='now', value='now')

            try:
                datetime.strptime(t['now'] or '', '%Y-%m-%d %H:%M:%S').astimezone()
            except ValueError as e:
                pytest.fail(str(e))

            del t['now']

            t = AtomicDate(scenario=scenario1, variable='now', value='now|format="%Y-%m-%d %H:%M:%S.%f"')
            first = t['now']
            gevent.sleep(0.1)

            assert first != t['now']

            del t['now']

            t = AtomicDate(scenario=scenario1, variable='now', value='now | format="%Y-%m-%d %H:%M:%S.000Z"')

            with pytest.raises(ValueError, match='argument calendar is not allowed'):
                AtomicDate(scenario=scenario1, variable='now', value='now | format="%Y-%m-%d %H:%M:%S.000Z", calendar="gregorian"')
        finally:
            cleanup()

    def test_format(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario

        try:
            expected = datetime.now()

            t = AtomicDate(scenario=scenario1, variable='actual', value='now')

            with pytest.raises(NotImplementedError, match='AtomicDate has not implemented "__setitem__"'):
                t['actual'] = None

            assert t['actual'] != expected.strftime('%Y-%m-%d %H:%M:%S.%f')

            del t['actual']
            del t['actual']

            with pytest.raises(AttributeError, match='AtomicDate object has no attribute "actual"'):
                t['actual']

            value = expected.strftime('%Y-%m-%d %H:%M:%S.%f')
            t = AtomicDate(scenario=scenario1, variable='actual', value=f'{value}|format="%Y-%m-%d"')

            assert t['actual'] is not None
            assert t['actual'] != expected.strftime('%Y-%m-%d %H:%M:%S.%f')
            del t['actual']

            t = AtomicDate(scenario=scenario1, variable='actual', value=f'{value}|format="%Y-%m-%d %H:%M:%S.%f"')
            assert t['actual'] is not None
            assert t['actual'] == expected.strftime('%Y-%m-%d %H:%M:%S.%f')
            del t['actual']

            with pytest.raises(ValueError, match='Unknown string format: asdfasdf'):
                AtomicDate(scenario=scenario1, variable='actual', value='asdfasdf|format="%Y-%m-%d %H:%M:%S.%f"')

            t = AtomicDate(scenario=scenario1, variable='actual', value='now | format="%Y"')
            assert t['actual'] == datetime.now().strftime('%Y')
            del t['actual']

            t = AtomicDate(scenario=scenario1, variable='actual', value="now | format='%Y-%m-%d %H'")
            assert t['actual'] == datetime.now().strftime('%Y-%m-%d %H')

            t = AtomicDate(scenario=scenario1, variable='test', value='now')
            assert len(t._settings.keys()) == 2
            assert 'test' in t._settings
            assert 'actual' in t._settings
        finally:
            cleanup()

    def test_timezone(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        try:
            expected_utc = datetime.now(tz=ZoneInfo('UTC')).strftime('%H:%M')
            expected_local = datetime.now().astimezone().strftime('%H:%M')

            t = AtomicDate(scenario=scenario1, variable='actual', value='now | format="%H:%M", timezone=UTC')
            assert t['actual'] == expected_utc
            assert t['actual'] != expected_local

            with pytest.raises(ValueError, match='date format is not specified'):
                AtomicDate(scenario=scenario1, variable='test', value='now | timezone=ASDF')
        finally:
            cleanup()

    def test_offset(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario

        try:
            expected = (datetime.now() + relativedelta(days=1)).strftime('%Y-%m-%d')

            t = AtomicDate(scenario=scenario1, variable='actual', value='now | format="%Y-%m-%d", offset=1D')
            assert t['actual'] == expected
            del t['actual']

            expected = (datetime.now() + relativedelta(years=-10, months=2, days=-2)).strftime('%Y-%m-%d')

            t = AtomicDate(scenario=scenario1, variable='actual', value='now | format="%Y-%m-%d", offset=-10Y2M-2D')
            assert t['actual'] == expected
            del t['actual']

            expected = '2017-10-12'
            t = AtomicDate(scenario=scenario1, variable='actual', value='2017-10-26 | format="%Y-%m-%d", offset=-14D')
            assert t['actual'] == expected

            with pytest.raises(ValueError, match='invalid time span format'):
                AtomicDate(scenario=scenario1, variable='error', value='now | format="%Y", offset=10L')
        finally:
            cleanup()

    def test_clear_and_destory(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            with suppress(Exception):
                AtomicDate.destroy()

            with pytest.raises(ValueError, match='is not instantiated'):
                AtomicDate.destroy()

            with pytest.raises(ValueError, match='is not instantiated'):
                AtomicDate.clear()

            expected = datetime.now()

            instances = [
                AtomicDate(scenario=scenario1, variable='actual', value='now'),
                AtomicDate(scenario=scenario2, variable='actual', value='now'),
            ]

            for instance in instances:
                assert instance['actual'] != expected.strftime('%Y-%m-%d %H:%M:%S.%f')

                assert len(instance._values.keys()) == 1
                assert len(instance._settings.keys()) == 1

            AtomicDate.clear()

            for instance in instances:
                assert len(instance._values.keys()) == 0
                assert len(instance._settings.keys()) == 0
        finally:
            cleanup()

    def test___getitem__error(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        mocker.patch(
            'grizzly.testdata.variables.date.AtomicDate._get_value',
            return_value=None,
        )

        grizzly = grizzly_fixture.grizzly

        try:
            t = AtomicDate(scenario=grizzly.scenario, variable='test', value='now | format="%Y-%m-%d"')

            with pytest.raises(ValueError, match='was incorrectly initialized with'):
                t['test']
        finally:
            cleanup()
