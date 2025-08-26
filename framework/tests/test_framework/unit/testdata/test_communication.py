"""Unit tests of grizzly.testdata.communication."""

from __future__ import annotations

import json
import logging
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from os import environ, sep
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import uuid4

import pytest
from gevent.event import AsyncResult
from gevent.lock import Semaphore
from grizzly.tasks import LogMessageTask
from grizzly.testdata.communication import AsyncTimer, AsyncTimersConsumer, AsyncTimersProducer, TestdataConsumer, TestdataProducer
from grizzly.testdata.utils import initialize_testdata, transform
from grizzly.testdata.variables import AtomicIntegerIncrementer
from grizzly.testdata.variables.csv_writer import atomiccsvwriter_message_handler
from grizzly.types.locust import Environment, LocalRunner, MasterRunner, Message, StopUser, WorkerRunner

from test_framework.helpers import ANY, ANYUUID, SOME

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from unittest.mock import MagicMock

    from _pytest.logging import LogCaptureFixture
    from grizzly.types import StrDict

    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture, LocustFixture, MockerFixture


def echo(value: StrDict) -> StrDict:
    return {'data': None, **value}


def echo_add_data(return_value: Any | list[Any]) -> Callable[[StrDict], StrDict]:
    if not isinstance(return_value, list):
        return_value = [return_value]

    def wrapped(request: StrDict) -> StrDict:
        return {'data': return_value.pop(0), **request}

    return wrapped


@pytest.fixture
def static_date() -> datetime:
    return datetime(2024, 12, 3, 8, 56, 55, 0).astimezone()


class TestAsyncTimer:
    def test___init__(self, static_date: datetime) -> None:
        timer = AsyncTimer('name', 'tid', 'version', start=static_date)

        assert timer == SOME(AsyncTimer, name='name', tid='tid', version='version', start=static_date, stop=None)

        timer = AsyncTimer('name', 'tid', 'version', stop=static_date)

        assert timer == SOME(AsyncTimer, name='name', tid='tid', version='version', start=None, stop=static_date)

    def test_is_complete(self, static_date: datetime) -> None:
        timer = AsyncTimer('name', 'tid', 'version')
        assert not timer.is_complete()

        timer.start = static_date
        assert not timer.is_complete()

        timer.start = None
        timer.stop = static_date
        assert not timer.is_complete()

        timer.start = static_date
        assert timer.is_complete()

    def test_complete(self, static_date: datetime, locust_fixture: LocustFixture, mocker: MockerFixture) -> None:
        stop = datetime(2024, 12, 3, 8, 56, 59, 0).astimezone()
        timer = AsyncTimer('name', 'tid', 'version')

        environment = locust_fixture.environment

        fire_spy = mocker.spy(environment.events.request, 'fire')

        timer.complete(environment.events.request)

        fire_spy.assert_called_once_with(
            request_type=AsyncTimersProducer.__request_method__,
            name='name',
            response_time=0,
            response_length=0,
            exception='cannot complete timer for id "tid" and version "version", missing start, stop timestamp',
            context={
                '__time__': None,
                '__fields_request_started__': None,
                '__fields_request_finished__': None,
            },
        )
        fire_spy.reset_mock()

        timer.start = static_date
        timer.complete(environment.events.request)

        fire_spy.assert_called_once_with(
            request_type=AsyncTimersProducer.__request_method__,
            name='name',
            response_time=0,
            response_length=0,
            exception='cannot complete timer for id "tid" and version "version", missing stop timestamp',
            context={
                '__time__': static_date.isoformat(),
                '__fields_request_started__': static_date.isoformat(),
                '__fields_request_finished__': None,
            },
        )
        fire_spy.reset_mock()

        timer.start = None
        timer.stop = stop
        timer.complete(environment.events.request)

        fire_spy.assert_called_once_with(
            request_type=AsyncTimersProducer.__request_method__,
            name='name',
            response_time=0,
            response_length=0,
            exception='cannot complete timer for id "tid" and version "version", missing start timestamp',
            context={
                '__time__': None,
                '__fields_request_started__': None,
                '__fields_request_finished__': stop.isoformat(),
            },
        )
        fire_spy.reset_mock()

        timer.start = static_date
        timer.complete(environment.events.request)

        fire_spy.assert_called_once_with(
            request_type=AsyncTimersProducer.__request_method__,
            name='name',
            response_time=4000,
            response_length=0,
            exception=None,
            context={
                '__time__': static_date.isoformat(),
                '__fields_request_started__': static_date.isoformat(),
                '__fields_request_finished__': stop.isoformat(),
            },
        )
        fire_spy.reset_mock()

        stop = stop + timedelta(minutes=2)
        timer.stop = stop
        timer.complete(environment.events.request)

        fire_spy.assert_called_once_with(
            request_type=AsyncTimersProducer.__request_method__,
            name='name',
            response_time=124000,
            response_length=0,
            exception=None,
            context={
                '__time__': static_date.isoformat(),
                '__fields_request_started__': static_date.isoformat(),
                '__fields_request_finished__': stop.isoformat(),
            },
        )
        fire_spy.reset_mock()


class TestAsyncTimersConsumer:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        semaphore = Semaphore()

        timers = AsyncTimersConsumer(parent, semaphore)
        assert timers.semaphore is semaphore
        assert timers.scenario is parent
        assert timers._start == []
        assert timers._stop == []

    def test_parse_date(self) -> None:
        assert AsyncTimersConsumer.parse_date('2024-12-03T09:02:29.000Z') == datetime(2024, 12, 3, 9, 2, 29, 0, tzinfo=timezone.utc)
        assert AsyncTimersConsumer.parse_date('2024-12-03 09:02:29').isoformat() == datetime(2024, 12, 3, 9, 2, 29).astimezone().isoformat()

    def test_on_report_to_master(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()
        semaphore_mock = mocker.MagicMock(spec=Semaphore)
        timers = AsyncTimersConsumer(parent, semaphore_mock)

        data = {
            'stats': {},
            'errors': [],
            'async_timers': {
                'start': [{'n': 'd'}, {'n': 'e'}, {'n': 'f'}],
                'stop': [{'n': 'd'}, {'n': 'e'}, {'n': 'f'}],
            },
        }

        timers._start = [{'n': 'd'}, {'n': 'e'}, {'n': 'f'}]
        timers._stop = [{'n': 'd'}, {'n': 'e'}, {'n': 'f'}]

        timers.on_report_to_master('local', data)

        assert data == {
            'stats': {},
            'errors': [],
            'async_timers': {
                'start': [{'n': 'd'}, {'n': 'e'}, {'n': 'f'}, {'n': 'd'}, {'n': 'e'}, {'n': 'f'}],
                'stop': [{'n': 'd'}, {'n': 'e'}, {'n': 'f'}, {'n': 'd'}, {'n': 'e'}, {'n': 'f'}],
            },
        }
        semaphore_mock.__enter__.assert_called_once_with()
        semaphore_mock.__exit__.assert_called_once_with(None, None, None)

        assert timers._start == []
        assert timers._stop == []

    def test_toggle(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, static_date: datetime) -> None:
        datetime_mock = mocker.patch('grizzly.testdata.communication.datetime', side_effect=lambda *args, **kwargs: datetime(*args, **kwargs))  # noqa: DTZ001
        datetime_mock.now.return_value = static_date

        parent = grizzly_fixture()
        semaphore_mock = mocker.MagicMock(spec=Semaphore)
        timers = AsyncTimersConsumer(parent, semaphore_mock)

        start_mock = mocker.patch.object(timers, 'start', return_value=None)
        stop_mock = mocker.patch.object(timers, 'stop', return_value=None)

        # <!-- start, provided timestamp
        timers.toggle('start', 'timer-1', 'foobar', '1', '2024-12-03 09:36:45.1234+01:00')

        start_mock.assert_called_once_with({'name': 'timer-1', 'tid': 'foobar', 'version': '1', 'timestamp': '2024-12-03T09:36:45.123400+01:00'})
        start_mock.reset_mock()
        stop_mock.assert_not_called()
        semaphore_mock.assert_not_called()

        timers.toggle('start', 'timer-2', 'barfoo', '1', '2024-12-03 09:39:49.1234+02:00')

        start_mock.assert_called_once_with({'name': 'timer-2', 'tid': 'barfoo', 'version': '1', 'timestamp': '2024-12-03T09:39:49.123400+02:00'})
        start_mock.reset_mock()
        stop_mock.assert_not_called()
        semaphore_mock.assert_not_called()
        # // -->

        # <!-- start, no timestamp
        timers.toggle('start', 'timer-1', 'foobar', '1')

        start_mock.assert_called_once_with({'name': 'timer-1', 'tid': 'foobar', 'version': '1', 'timestamp': static_date.isoformat()})
        start_mock.reset_mock()
        stop_mock.assert_not_called()
        semaphore_mock.assert_not_called()

        timers.toggle('start', 'timer-2', 'barfoo', '1')

        start_mock.assert_called_once_with({'name': 'timer-2', 'tid': 'barfoo', 'version': '1', 'timestamp': static_date.isoformat()})
        start_mock.reset_mock()
        stop_mock.assert_not_called()
        semaphore_mock.assert_not_called()
        # // -->

        # <!-- stop, provided timestamp
        timers.toggle('stop', 'timer-1', 'foobar', '1', '2024-12-03 09:47:59')

        start_mock.assert_not_called()
        stop_mock.assert_called_once_with({'name': 'timer-1', 'tid': 'foobar', 'version': '1', 'timestamp': '2024-12-03T09:47:59+01:00'})
        stop_mock.reset_mock()
        semaphore_mock.assert_not_called()

        timers.toggle('stop', 'timer-2', 'barfoo', '1', '2024-12-03 09:47:59+01:00')

        start_mock.assert_not_called()
        stop_mock.assert_called_once_with({'name': 'timer-2', 'tid': 'barfoo', 'version': '1', 'timestamp': '2024-12-03T09:47:59+01:00'})
        stop_mock.reset_mock()
        semaphore_mock.reset_mock()
        # // -->

    @pytest.mark.parametrize('target', ['start', 'stop'])
    def test_action(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, target: Literal['start', 'stop']) -> None:
        other = 'stop' if target == 'start' else 'start'

        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly
        semaphore_mock = mocker.MagicMock(spec=Semaphore)
        timers = AsyncTimersConsumer(parent, semaphore_mock)

        async_timers_mock = mocker.MagicMock(spec=AsyncTimersProducer)
        producer_mock = mocker.MagicMock(spec=TestdataProducer)
        producer_mock.async_timers = async_timers_mock

        grizzly.state.producer = producer_mock

        # <!-- LocalRunner
        timestamp = datetime.now().astimezone()
        timers.toggle(target, 'name', 'tid', 'version', timestamp)
        producer_mock.async_timers.toggle.assert_called_once_with(target, {'name': 'name', 'tid': 'tid', 'version': 'version', 'timestamp': timestamp.isoformat()})
        producer_mock.reset_mock()

        assert getattr(timers, f'_{target}') == []
        assert getattr(timers, f'_{other}') == []
        # // -->

        # <!-- not LocalRunner
        runner_classes: list[type[WorkerRunner | MasterRunner | LocalRunner]] = [WorkerRunner, MasterRunner]
        for runner_class in runner_classes:
            grizzly.state.locust.__class__ = runner_class
            timestamp = datetime.now().astimezone()
            timers.toggle(target, 'name', 'tid', 'version', timestamp)
            producer_mock.async_timers.toggle.assert_not_called()
            producer_mock.reset_mock()
            assert getattr(timers, f'_{target}') == [{'name': 'name', 'tid': 'tid', 'version': 'version', 'timestamp': timestamp.isoformat()}]
            assert getattr(timers, f'_{other}') == []
            getattr(timers, f'_{target}').clear()
        # // -->


class TestAsyncTimersProducer:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        semaphore = Semaphore()

        timers = AsyncTimersProducer(grizzly, semaphore)
        assert timers.semaphore is semaphore
        assert timers.grizzly is grizzly
        assert timers.timers == {}

    def test_extract(self) -> None:
        timestamp = datetime(2024, 12, 3, 9, 9, 17).astimezone()

        data = {
            'name': 'timer-1',
            'tid': 'foobar',
            'version': '1',
            'timestamp': timestamp.isoformat(),
        }

        assert AsyncTimersProducer.extract(data) == ('timer-1', 'foobar', '1', timestamp)

    def test_on_worker_report(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        semaphore_mock = mocker.MagicMock(spec=Semaphore)
        logger = logging.getLogger('test')
        grizzly.state.producer = mocker.MagicMock(spec=TestdataProducer)
        cast('TestdataProducer', grizzly.state.producer).logger = logger
        timers = AsyncTimersProducer(grizzly, semaphore_mock)

        toggle_mock = mocker.patch.object(timers, 'toggle', return_value=None)

        timers.on_worker_report('local', {})

        toggle_mock.assert_not_called()

        data = {
            'stats': {},
            'errors': [],
            'async_timers': {
                'start': [{'n': 'd'}, {'n': 'e'}, {'n': 'f'}],
                'stop': [{'n': 'd'}, {'n': 'e'}, {'n': 'f'}],
            },
        }

        timers.on_worker_report('local', data)

        assert toggle_mock.call_count == 6
        assert toggle_mock.call_args_list[0] == (('start', {'n': 'd'}), {})
        assert toggle_mock.call_args_list[1] == (('start', {'n': 'e'}), {})
        assert toggle_mock.call_args_list[2] == (('start', {'n': 'f'}), {})
        assert toggle_mock.call_args_list[3] == (('stop', {'n': 'd'}), {})
        assert toggle_mock.call_args_list[4] == (('stop', {'n': 'e'}), {})
        assert toggle_mock.call_args_list[5] == (('stop', {'n': 'f'}), {})

        semaphore_mock.assert_not_called()

    def test_toggle(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, static_date: datetime, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        grizzly = grizzly_fixture.grizzly
        semaphore_mock = mocker.MagicMock(spec=Semaphore)
        logger = logging.getLogger('test')
        grizzly.state.producer = mocker.MagicMock(spec=TestdataProducer)
        cast('TestdataProducer', grizzly.state.producer).logger = logger
        timers = AsyncTimersProducer(grizzly, semaphore_mock)

        fire_mock = mocker.spy(grizzly.state.locust.environment.events.request, 'fire')
        log_error_mock = mocker.spy(grizzly.state.locust.stats, 'log_error')

        # <!-- start
        timers.toggle('start', {'name': 'timer-2', 'tid': 'foobar', 'version': 'a', 'timestamp': static_date.isoformat()})
        timers.toggle('start', {'name': 'timer-1', 'tid': 'foobar', 'version': 'a', 'timestamp': static_date.isoformat()})

        assert timers.timers == {
            'timer-1::foobar::a': SOME(AsyncTimer, name='timer-1', tid='foobar', version='a', start=static_date, stop=None),
            'timer-2::foobar::a': SOME(AsyncTimer, name='timer-2', tid='foobar', version='a', start=static_date, stop=None),
        }

        semaphore_mock.reset_mock()

        with caplog.at_level(logging.ERROR):
            timers.toggle('start', {'name': 'timer-1', 'tid': 'foobar', 'version': 'a', 'timestamp': static_date.isoformat()})

        assert caplog.messages == ['timer with name "timer-1" for id "foobar" with version "a" has already been started']
        caplog.clear()
        log_error_mock.assert_called_once_with('DOC', 'timer-1', 'timer for id "foobar" with version "a" has already been started')
        log_error_mock.reset_mock()

        assert timers.timers == {
            'timer-1::foobar::a': SOME(AsyncTimer, name='timer-1', tid='foobar', version='a', start=static_date, stop=None),
            'timer-2::foobar::a': SOME(AsyncTimer, name='timer-2', tid='foobar', version='a', start=static_date, stop=None),
        }

        semaphore_mock.__enter__.assert_not_called()
        semaphore_mock.__exit__.assert_not_called()

        del timers.timers['timer-1::foobar::a']

        timers.toggle('start', {'name': 'timer-1', 'tid': 'foobar', 'version': 'a', 'timestamp': static_date.isoformat()})
        semaphore_mock.__enter__.assert_called_once_with()
        semaphore_mock.__exit__.assert_called_once_with(None, None, None)
        semaphore_mock.reset_mock()

        fire_mock.assert_not_called()
        # // -->

        # <!-- stop
        stop_date = static_date + timedelta(seconds=10)

        timers.toggle('stop', {'name': 'timer-3', 'tid': 'barfoo', 'version': 'a', 'timestamp': stop_date.isoformat()})

        with caplog.at_level(logging.ERROR):
            timers.toggle('stop', {'name': 'timer-3', 'tid': 'barfoo', 'version': 'a', 'timestamp': stop_date.isoformat()})

        assert caplog.messages == ['timer with name "timer-3" for id "barfoo" with version "a" has already been stopped']
        caplog.clear()
        log_error_mock.assert_called_once_with('DOC', 'timer-3', 'timer for id "barfoo" with version "a" has already been stopped')
        log_error_mock.reset_mock()

        semaphore_mock.__enter__.assert_called_once_with()
        semaphore_mock.__exit__.assert_called_once_with(None, None, None)
        semaphore_mock.reset_mock()

        assert timers.timers == {
            'timer-1::foobar::a': SOME(AsyncTimer, name='timer-1', tid='foobar', version='a', start=static_date, stop=None),
            'timer-2::foobar::a': SOME(AsyncTimer, name='timer-2', tid='foobar', version='a', start=static_date, stop=None),
            'timer-3::barfoo::a': SOME(AsyncTimer, name='timer-3', tid='barfoo', version='a', start=None, stop=stop_date),
        }

        timers.toggle('stop', {'name': 'timer-1', 'tid': 'foobar', 'version': 'a', 'timestamp': stop_date.isoformat()})
        assert timers.timers == {
            'timer-2::foobar::a': SOME(AsyncTimer, name='timer-2', tid='foobar', version='a', start=static_date, stop=None),
            'timer-3::barfoo::a': SOME(AsyncTimer, name='timer-3', tid='barfoo', version='a', start=None, stop=stop_date),
        }

        semaphore_mock.__enter__.assert_called_once_with()
        semaphore_mock.__exit__.assert_called_once_with(None, None, None)
        semaphore_mock.reset_mock()

        fire_mock.assert_called_once_with(
            request_type=AsyncTimersProducer.__request_method__,
            name='timer-1',
            response_time=10000,
            response_length=0,
            exception=None,
            context={
                '__time__': static_date.isoformat(),
                '__fields_request_started__': static_date.isoformat(),
                '__fields_request_finished__': stop_date.isoformat(),
            },
        )
        fire_mock.reset_mock()
        log_error_mock.assert_not_called()

        timers.toggle('stop', {'name': 'timer-2', 'tid': 'foobar', 'version': 'a', 'timestamp': stop_date.isoformat()})
        assert timers.timers == {
            'timer-3::barfoo::a': SOME(AsyncTimer, name='timer-3', tid='barfoo', version='a', start=None, stop=stop_date),
        }

        semaphore_mock.__enter__.assert_called_once_with()
        semaphore_mock.__exit__.assert_called_once_with(None, None, None)
        semaphore_mock.reset_mock()

        fire_mock.assert_called_once_with(
            request_type=AsyncTimersProducer.__request_method__,
            name='timer-2',
            response_time=10000,
            response_length=0,
            exception=None,
            context={
                '__time__': static_date.isoformat(),
                '__fields_request_started__': static_date.isoformat(),
                '__fields_request_finished__': stop_date.isoformat(),
            },
        )
        fire_mock.reset_mock()

        timers.toggle('start', {'name': 'timer-3', 'tid': 'barfoo', 'version': 'a', 'timestamp': static_date.isoformat()})
        assert timers.timers == {}

        semaphore_mock.__enter__.assert_called_once_with()
        semaphore_mock.__exit__.assert_called_once_with(None, None, None)
        semaphore_mock.reset_mock()

        fire_mock.assert_called_once_with(
            request_type=AsyncTimersProducer.__request_method__,
            name='timer-3',
            response_time=10000,
            response_length=0,
            exception=None,
            context={
                '__time__': static_date.isoformat(),
                '__fields_request_started__': static_date.isoformat(),
                '__fields_request_finished__': stop_date.isoformat(),
            },
        )
        fire_mock.reset_mock()
        # // -->


class TestTestdataProducer:
    def test_run(  # noqa: PLR0915
        self,
        grizzly_fixture: GrizzlyFixture,
        cleanup: AtomicVariableCleanupFixture,
        caplog: LogCaptureFixture,
        mocker: MockerFixture,
    ) -> None:
        request = grizzly_fixture.request_task.request

        success = False

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run.feature'
        context_root = grizzly_fixture.test_context / 'requests'
        context_root.mkdir(exist_ok=True)

        try:
            parent = grizzly_fixture()

            (context_root / 'adirectory').mkdir()

            for index in range(1, 3):
                value = f'file{index}.txt'
                (context_root / 'adirectory' / value).write_text(f'{value}\n')

            (context_root / 'test.csv').write_text("""header1,header2
value1,value2
value3,value4
""")

            with (context_root / 'test.json').open('w') as fd:
                json.dump([{'header1': 'value1', 'header2': 'value2'}, {'header1': 'value3', 'header2': 'value4'}], fd)

            assert request.source is not None

            source = json.loads(request.source)
            source['result'].update(
                {
                    'File': '{{ AtomicDirectoryContents.test }}',
                    'CsvRowValue1': '{{ AtomicCsvReader.test.header1 }}',
                    'CsvRowValue2': '{{ AtomicCsvReader.test.header2 }}',
                    'JsonRowValue1': '{{ AtomicJsonReader.test.header1 }}',
                    'JsonRowValue2': '{{ AtomicJsonReader.test.header2 }}',
                    'JsonRowValue': '{{ AtomicJsonReader.test2 }}',
                    'IntWithStep': '{{ AtomicIntegerIncrementer.value }}',
                    'UtcDate': '{{ AtomicDate.utc }}',
                    'CustomVariable': '{{ test_framework.helpers.AtomicCustomVariable.foo }}',
                },
            )

            grizzly = grizzly_fixture.grizzly

            testdata_request_spy = mocker.spy(grizzly.events.testdata_request, 'fire')
            keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

            grizzly.scenarios.clear()
            grizzly.scenarios.create(grizzly_fixture.behave.create_scenario(parent.__class__.__name__))
            grizzly.scenario.orphan_templates.append('{{ AtomicCsvWriter.output }}')
            grizzly.scenario.variables.update(
                {
                    'messageID': 123,
                    'AtomicIntegerIncrementer.messageID': 456,
                    'AtomicDirectoryContents.test': 'adirectory',
                    'AtomicCsvReader.test': 'test.csv',
                    'AtomicJsonReader.test': 'test.json',
                    'AtomicJsonReader.test2': 'test.json',
                    'AtomicCsvWriter.output': 'output.csv | headers="foo,bar"',
                    'AtomicIntegerIncrementer.value': '1 | step=5, persist=True',
                    'AtomicDate.utc': "now | format='%Y-%m-%dT%H:%M:%S.000Z', timezone=UTC",
                    'AtomicDate.now': 'now',
                    'world': 'hello!',
                    'test_framework.helpers.AtomicCustomVariable.foo': 'bar',
                },
            )
            grizzly.scenario.variables.alias.update(
                {
                    'AtomicCsvReader.test.header1': 'auth.user.username',
                    'AtomicCsvReader.test.header2': 'auth.user.password',
                },
            )
            grizzly.scenario.iterations = 2
            grizzly.scenario.user.class_name = 'TestUser'
            grizzly.scenario.context['host'] = 'http://test.nu'

            request.source = json.dumps(source)

            grizzly.scenario.tasks.add(request)
            grizzly.scenario.tasks.add(LogMessageTask(message='hello {{ world }}'))

            testdata, dependencies = initialize_testdata(grizzly)

            assert dependencies == {('atomiccsvwriter', atomiccsvwriter_message_handler)}

            grizzly.state.producer = TestdataProducer(
                runner=cast('LocalRunner', grizzly.state.locust),
                testdata=testdata,
            )

            assert grizzly.state.locust.custom_messages.get('produce_testdata', None) == (grizzly.state.producer.handle_request, True)
            assert isinstance(grizzly.state.producer.async_timers, AsyncTimersProducer)
            assert grizzly.state.producer.async_timers.on_worker_report not in grizzly.state.locust.environment.events.worker_report._handlers

            assert grizzly.state.producer.keystore == {}

            responses: dict[int, AsyncResult] = {}

            def handle_consume_data(*, environment: Environment, msg: Message) -> None:  # noqa: ARG001
                uid = msg.data['uid']
                response = msg.data['response']
                responses[uid].set(response)

            grizzly.state.locust.register_message('consume_testdata', handle_consume_data)

            def request_testdata() -> StrDict | None:
                uid = id(parent.user)
                rid = str(uuid4())

                responses[uid] = AsyncResult()
                grizzly.state.locust.send_message(
                    'produce_testdata',
                    {
                        'uid': uid,
                        'cid': cast('LocalRunner', grizzly.state.locust).client_id,
                        'rid': rid,
                        'request': {'message': 'testdata', 'identifier': grizzly.scenario.class_name},
                    },
                )

                response = cast('StrDict | None', responses[uid].get())

                del responses[uid]

                return response

            def request_keystore(action: str, key: str, value: Any | None = None) -> StrDict | None:
                uid = id(parent.user)
                rid = str(uuid4())

                request = {
                    'message': 'keystore',
                    'identifier': grizzly.scenario.class_name,
                    'action': action,
                    'key': key,
                }

                if value is not None:
                    request.update({'data': value})

                responses[uid] = AsyncResult()
                grizzly.state.locust.send_message('produce_testdata', {'uid': uid, 'cid': cast('LocalRunner', grizzly.state.locust).client_id, 'rid': rid, 'request': request})

                response = cast('StrDict | None', responses[uid].get())

                del responses[uid]

                return response

            response = request_testdata()
            assert response is not None
            testdata_request_spy.assert_called_once_with(
                reverse=False,
                timestamp=ANY(str),
                tags={
                    'action': 'consume',
                    'type': 'producer',
                    'identifier': grizzly.scenario.class_name,
                },
                measurement='request_testdata',
                metrics={
                    'response_time': ANY(float),
                    'error': None,
                },
            )
            testdata_request_spy.reset_mock()
            keystore_request_spy.assert_not_called()
            assert response['action'] == 'consume'
            data = response['data']
            assert 'variables' in data
            variables = data['variables']
            assert variables == {
                'AtomicIntegerIncrementer.messageID': 456,
                'AtomicDate.now': ANY(str),
                'messageID': 123,
                'AtomicDirectoryContents.test': f'adirectory{sep}file1.txt',
                'AtomicDate.utc': ANY(str),
                'AtomicCsvReader.test.header1': 'value1',
                'AtomicCsvReader.test.header2': 'value2',
                'AtomicJsonReader.test.header1': 'value1',
                'AtomicJsonReader.test.header2': 'value2',
                'AtomicJsonReader.test2': {'header1': 'value1', 'header2': 'value2'},
                'AtomicIntegerIncrementer.value': 1,
                'test_framework.helpers.AtomicCustomVariable.foo': 'bar',
                'world': 'hello!',
            }
            assert data == {
                'variables': variables,
                'auth.user.username': 'value1',
                'auth.user.password': 'value2',
                '__iteration__': (0, 2),
            }
            assert grizzly.state.producer is not None
            assert grizzly.state.producer.keystore == {}

            response = request_keystore('set', 'foobar', {'hello': 'world'})
            assert response is not None
            testdata_request_spy.assert_not_called()
            keystore_request_spy.assert_called_once_with(
                reverse=False,
                timestamp=ANY(str),
                tags={
                    'identifier': grizzly.scenario.class_name,
                    'action': 'set',
                    'key': 'foobar',
                    'type': 'producer',
                },
                measurement='request_keystore',
                metrics={
                    'response_time': ANY(float),
                    'error': None,
                },
            )
            assert response == {
                'message': 'keystore',
                'action': 'set',
                'identifier': grizzly.scenario.class_name,
                'key': 'foobar',
                'data': {'hello': 'world'},
            }

            response = request_testdata()
            assert response is not None
            assert response['action'] == 'consume'
            data = response['data']
            assert 'variables' in data
            variables = data['variables']
            assert 'AtomicIntegerIncrementer.messageID' in variables
            assert 'AtomicDate.now' in variables
            assert 'messageID' in variables
            assert variables['AtomicIntegerIncrementer.messageID'] == 457
            assert variables['messageID'] == 123
            assert variables['AtomicDirectoryContents.test'] == f'adirectory{sep}file2.txt'
            assert variables['AtomicCsvReader.test.header1'] == 'value3'
            assert variables['AtomicCsvReader.test.header2'] == 'value4'
            assert variables['AtomicJsonReader.test.header1'] == 'value3'
            assert variables['AtomicJsonReader.test.header2'] == 'value4'
            assert variables['AtomicJsonReader.test2'] == {'header1': 'value3', 'header2': 'value4'}
            assert variables['AtomicIntegerIncrementer.value'] == 6
            assert data['auth.user.username'] == 'value3'
            assert data['auth.user.password'] == 'value4'

            response = request_keystore('get', 'foobar')
            assert response == {
                'message': 'keystore',
                'action': 'get',
                'identifier': grizzly.scenario.class_name,
                'key': 'foobar',
                'data': {'hello': 'world'},
            }

            caplog.clear()

            with caplog.at_level(logging.ERROR):
                response = request_keystore('inc', 'counter', 1)

            assert response == {
                'message': 'keystore',
                'action': 'inc',
                'identifier': grizzly.scenario.class_name,
                'key': 'counter',
                'data': 1,
            }

            assert caplog.messages == []

            caplog.clear()

            grizzly.state.producer.keystore.update({'counter': 'asdf'})

            with caplog.at_level(logging.ERROR):
                response = request_keystore('inc', 'counter', 1)

            assert response == {
                'message': 'keystore',
                'action': 'inc',
                'identifier': grizzly.scenario.class_name,
                'key': 'counter',
                'data': None,
                'error': 'value asdf for key "counter" cannot be incremented',
            }

            assert caplog.messages == ['value asdf for key "counter" cannot be incremented']

            caplog.clear()
            grizzly.state.producer.keystore.update({'counter': 1})

            with caplog.at_level(logging.ERROR):
                response = request_keystore('inc', 'counter', 1)

            assert response == {
                'message': 'keystore',
                'action': 'inc',
                'identifier': grizzly.scenario.class_name,
                'key': 'counter',
                'data': 2,
            }

            assert caplog.messages == []

            with caplog.at_level(logging.ERROR):
                response = request_keystore('inc', 'counter', 10)

            assert response == {
                'message': 'keystore',
                'action': 'inc',
                'identifier': grizzly.scenario.class_name,
                'key': 'counter',
                'data': 12,
            }

            assert caplog.messages == []

            with caplog.at_level(logging.ERROR):
                response = request_keystore('dec', 'counter', 2)

            assert response == {
                'message': 'keystore',
                'action': 'dec',
                'identifier': grizzly.scenario.class_name,
                'key': 'counter',
                'data': 10,
            }

            response = request_testdata()
            assert response is not None
            assert response['action'] == 'stop'
            assert 'data' not in response

            success = True
        finally:
            if grizzly.state.producer is not None:
                grizzly.state.producer.stop()

                persist_file = Path(context_root).parent / 'persistent' / 'test_run.json'
                assert persist_file.exists()

                if success:
                    actual_initial_values = json.loads(persist_file.read_text())
                    assert actual_initial_values == {
                        'IteratorScenario_001': {
                            'AtomicIntegerIncrementer.value': '11 | step=5, persist=True',
                        },
                    }

            cleanup()

    def test_run_variable_none(
        self,
        grizzly_fixture: GrizzlyFixture,
        cleanup: AtomicVariableCleanupFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        try:
            grizzly = grizzly_fixture.grizzly
            parent = grizzly_fixture()
            context_root = grizzly_fixture.test_context / 'requests'
            context_root.mkdir(exist_ok=True)
            request = grizzly_fixture.request_task.request
            environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_with_variable_none.feature'

            (context_root / 'adirectory').mkdir()

            assert request.source is not None

            source = json.loads(request.source)
            source['result'].update({'File': '{{ AtomicDirectoryContents.file }}'})

            request.source = json.dumps(source)

            grizzly.scenarios.clear()
            grizzly.scenarios.create(grizzly_fixture.behave.create_scenario(parent.__class__.__name__))
            grizzly.scenario.variables.update(
                {
                    'messageID': 123,
                    'AtomicIntegerIncrementer.messageID': 456,
                    'AtomicDirectoryContents.file': 'adirectory',
                    'AtomicDate.now': 'now',
                    'sure': 'no',
                },
            )
            grizzly.scenario.iterations = 0
            grizzly.scenario.user.class_name = 'TestUser'
            grizzly.scenario.context['host'] = 'http://test.example.com'
            grizzly.scenario.tasks.add(request)
            grizzly.scenario.tasks.add(LogMessageTask(message='are you {{ sure }}'))

            testdata, dependencies = initialize_testdata(grizzly)

            assert dependencies == set()

            grizzly.state.producer = TestdataProducer(
                runner=cast('LocalRunner', grizzly.state.locust),
                testdata=testdata,
            )

            responses: dict[int, AsyncResult] = {}

            def handle_consume_data(*, environment: Environment, msg: Message) -> None:  # noqa: ARG001
                uid = msg.data['uid']
                response = msg.data['response']
                responses[uid].set(response)

            grizzly.state.locust.register_message('consume_testdata', handle_consume_data)

            def request_testdata() -> StrDict | None:
                uid = id(parent.user)
                rid = str(uuid4())

                responses[uid] = AsyncResult()
                grizzly.state.locust.send_message(
                    'produce_testdata',
                    {
                        'uid': uid,
                        'cid': cast('LocalRunner', grizzly.state.locust).client_id,
                        'rid': rid,
                        'request': {'message': 'testdata', 'identifier': grizzly.scenario.class_name},
                    },
                )

                response = cast('StrDict | None', responses[uid].get())

                del responses[uid]

                return response

            with caplog.at_level(logging.DEBUG):
                response = request_testdata()
                assert response is not None
                assert response['action'] == 'stop'

        finally:
            if grizzly.state.producer is not None:
                grizzly.state.producer.stop()

                persist_file = Path(context_root).parent / 'persistent' / 'test_run_with_none.json'
                assert not persist_file.exists()

            cleanup()

    def test_on_stop(self, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture) -> None:
        try:
            grizzly = grizzly_fixture.grizzly
            environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_with_variable_none.feature'
            grizzly.state.producer = TestdataProducer(runner=cast('LocalRunner', grizzly.state.locust), testdata={})
            grizzly.state.producer.scenarios_iteration = {
                'test-scenario-1': 10,
                'test-scenario-2': 5,
            }

            grizzly.state.producer.on_test_stop(grizzly.state.locust.environment)

            for scenario, count in grizzly.state.producer.scenarios_iteration.items():
                assert count == 0, f'iteration count for {scenario} was not reset'
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']

            cleanup()

    def test_stop_exception(
        self,
        cleanup: AtomicVariableCleanupFixture,
        grizzly_fixture: GrizzlyFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        context_root = grizzly_fixture.test_context / 'requests'
        context_root.mkdir(exist_ok=True)

        persistent_file = grizzly_fixture.test_context / 'persistent' / 'test_run_with_variable_none.json'

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_with_variable_none.feature'

        try:
            with caplog.at_level(logging.DEBUG):
                TestdataProducer(cast('LocalRunner', grizzly.state.locust), {}).stop()

            del grizzly.state.locust.custom_messages['produce_testdata']

            assert caplog.messages == ['serving:\n{}', 'persisting test data...', 'no data to persist for feature file, skipping']
            assert not persistent_file.exists()

            i = AtomicIntegerIncrementer(scenario=scenario1, variable='foobar', value='1 | step=1, persist=True')
            j = AtomicIntegerIncrementer(scenario=scenario2, variable='foobar', value='10 | step=10, persist=True')

            for v in [i, j]:
                v['foobar']
                v['foobar']

            actual_keystore = {'foo': ['hello', 'world'], 'bar': {'hello': 'world', 'foo': 'bar'}, 'hello': 'world'}

            with caplog.at_level(logging.DEBUG):
                grizzly.state.producer = TestdataProducer(
                    cast('LocalRunner', grizzly.state.locust),
                    {
                        scenario1.class_name: {'AtomicIntegerIncrementer.foobar': i},
                        scenario2.class_name: {'AtomicIntegerIncrementer.foobar': j},
                    },
                )
                grizzly.state.producer.keystore.update(actual_keystore)
                grizzly.state.producer.stop()

            assert caplog.messages[-1] == f'feature file data persisted in {persistent_file}'
            caplog.clear()
            del grizzly.state.locust.custom_messages['produce_testdata']

            assert persistent_file.exists()

            actual_persist_values = json.loads(persistent_file.read_text())
            assert actual_persist_values == {
                'IteratorScenario_001': {
                    'AtomicIntegerIncrementer.foobar': '3 | step=1, persist=True',
                },
                'IteratorScenario_002': {
                    'AtomicIntegerIncrementer.foobar': '30 | step=10, persist=True',
                },
            }

            i['foobar']
            j['foobar']

            with caplog.at_level(logging.DEBUG):
                grizzly.state.producer = TestdataProducer(
                    cast('LocalRunner', grizzly.state.locust),
                    {
                        scenario1.class_name: {'AtomicIntegerIncrementer.foobar': i},
                        scenario2.class_name: {'AtomicIntegerIncrementer.foobar': j},
                    },
                )
                grizzly.state.producer.stop()

            assert caplog.messages[-1] == f'feature file data persisted in {persistent_file}'
            caplog.clear()
            del grizzly.state.locust.custom_messages['produce_testdata']

            assert persistent_file.exists()

            actual_persist_values = json.loads(persistent_file.read_text())
            assert actual_persist_values == {
                'IteratorScenario_001': {
                    'AtomicIntegerIncrementer.foobar': '5 | step=1, persist=True',
                },
                'IteratorScenario_002': {
                    'AtomicIntegerIncrementer.foobar': '50 | step=10, persist=True',
                },
            }
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']
            cleanup()

    def test_run_keystore(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly
        context_root = Path(grizzly_fixture.request_task.context_root).parent

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_keystore.feature'
        environ['GRIZZLY_CONTEXT_ROOT'] = context_root.as_posix()

        try:
            grizzly.state.producer = TestdataProducer(cast('LocalRunner', grizzly.state.locust), {})
            grizzly.state.producer.keystore.update({'hello': 'world'})

            responses: dict[int, AsyncResult] = {}

            def handle_consume_data(*, environment: Environment, msg: Message) -> None:  # noqa: ARG001
                uid = msg.data['uid']
                response = msg.data['response']
                responses[uid].set(response)

            grizzly.state.locust.register_message('consume_testdata', handle_consume_data)

            def request_keystore(action: str, key: str, value: Any | None = None, message: str = 'keystore') -> StrDict | None:
                uid = id(parent.user)
                rid = str(uuid4())

                request: StrDict = {
                    'message': message,
                    'identifier': grizzly.scenario.class_name,
                    'action': action,
                    'key': key,
                }

                if action.startswith('get'):
                    request.update({'action': 'get', 'remove': (action == 'get_del')})

                if value is not None:
                    request.update({'data': value})

                responses[uid] = AsyncResult()
                grizzly.state.locust.send_message(
                    'produce_testdata',
                    {
                        'uid': uid,
                        'cid': cast('LocalRunner', grizzly.state.locust).client_id,
                        'rid': rid,
                        'request': request,
                    },
                )

                response = cast('StrDict | None', responses[uid].get())

                del responses[uid]

                return response

            with caplog.at_level(logging.ERROR):
                response = request_keystore('get', 'hello', 'world')

            assert response == {'message': 'keystore', 'action': 'get', 'data': 'world', 'identifier': grizzly.scenario.class_name, 'key': 'hello', 'remove': False}
            assert caplog.messages == []

            grizzly.state.producer.keystore.clear()

            with caplog.at_level(logging.ERROR):
                response = request_keystore('set', 'world', {'foo': 'bar'})

            assert response == {'message': 'keystore', 'action': 'set', 'data': {'foo': 'bar'}, 'identifier': grizzly.scenario.class_name, 'key': 'world'}
            assert caplog.messages == []
            assert grizzly.state.producer.keystore == {'world': {'foo': 'bar'}}

            # <!-- push
            assert 'foobar' not in grizzly.state.producer.keystore

            response = request_keystore('push', 'foobar', 'foobar')

            assert response == {'message': 'keystore', 'action': 'push', 'data': 'foobar', 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            assert grizzly.state.producer.keystore['foobar'] == ['foobar']

            response = request_keystore('push', 'foobar', 'foobaz')

            assert response == {'message': 'keystore', 'action': 'push', 'data': 'foobaz', 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            assert grizzly.state.producer.keystore['foobar'] == ['foobar', 'foobaz']
            # // push -->

            # <!-- pop
            assert 'world' in grizzly.state.producer.keystore

            response = request_keystore('pop', 'world', 'foobar')

            assert response == {
                'message': 'keystore',
                'action': 'pop',
                'data': None,
                'identifier': grizzly.scenario.class_name,
                'key': 'world',
                'error': 'key "world" is not a list, it has not been pushed to',
            }

            response = request_keystore('pop', 'foobaz', 'foobar')

            assert response == {'message': 'keystore', 'action': 'pop', 'data': None, 'identifier': grizzly.scenario.class_name, 'key': 'foobaz'}

            response = request_keystore('pop', 'foobar')

            assert response == {'message': 'keystore', 'action': 'pop', 'data': 'foobar', 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            assert grizzly.state.producer.keystore['foobar'] == ['foobaz']

            response = request_keystore('pop', 'foobar')

            assert response == {'message': 'keystore', 'action': 'pop', 'data': 'foobaz', 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            with pytest.raises(KeyError):
                grizzly.state.producer.keystore['foobar']

            response = request_keystore('pop', 'foobar')

            assert response == {'message': 'keystore', 'action': 'pop', 'data': None, 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            # // pop -->

            # <!-- del
            grizzly.state.producer.keystore.update({'foobar': 'barfoo'})
            assert 'foobar' in grizzly.state.producer.keystore

            response = request_keystore('del', 'foobar', 'dummy')

            assert response == {'message': 'keystore', 'action': 'del', 'data': None, 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            assert 'foobar' not in grizzly.state.producer.keystore

            response = request_keystore('del', 'foobar', 'dummy')
            assert response == {
                'message': 'keystore',
                'action': 'del',
                'data': None,
                'identifier': grizzly.scenario.class_name,
                'key': 'foobar',
                'error': 'failed to remove key "foobar"',
            }
            # // del -->

            # <!-- get_del
            assert 'foobar' not in grizzly.state.producer.keystore
            response = request_keystore('get_del', 'foobar')

            assert response == {
                'message': 'keystore',
                'action': 'get',
                'remove': True,
                'data': None,
                'identifier': grizzly.scenario.class_name,
                'key': 'foobar',
                'error': 'failed to remove key "foobar"',
            }

            grizzly.state.producer.keystore.update({'foobar': 'hello world'})

            response = request_keystore('get_del', 'foobar')

            assert 'foobar' not in grizzly.state.producer.keystore
            assert response == {
                'message': 'keystore',
                'action': 'get',
                'remove': True,
                'data': 'hello world',
                'identifier': grizzly.scenario.class_name,
                'key': 'foobar',
            }
            # // get_del -->

            caplog.clear()

            response = request_keystore('unknown', 'asdf')
            assert response == {
                'message': 'keystore',
                'action': 'unknown',
                'data': None,
                'identifier': grizzly.scenario.class_name,
                'key': 'asdf',
                'error': 'received unknown keystore action "unknown"',
            }
            assert caplog.messages == ['received unknown keystore action "unknown"']
            caplog.clear()

            with caplog.at_level(logging.ERROR):
                response = request_keystore('get', 'foobar', None, message='unknown')

            assert response == {}
            assert caplog.messages == ['received unknown message "unknown"']
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']

            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

    def test_persist_data_edge_cases(
        self,
        mocker: MockerFixture,
        grizzly_fixture: GrizzlyFixture,
        caplog: LogCaptureFixture,
        cleanup: AtomicVariableCleanupFixture,
    ) -> None:
        context_root = grizzly_fixture.test_context / 'requests'
        context_root.mkdir(exist_ok=True)
        grizzly = grizzly_fixture.grizzly

        persistent_file = context_root / 'persistent' / 'test_run_with_variable_none.json'

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_persist_data_edge_cases.feature'

        try:
            assert not persistent_file.exists()
            i = AtomicIntegerIncrementer(scenario=grizzly.scenario, variable='foobar', value='1 | step=1, persist=True')

            grizzly.state.producer = TestdataProducer(
                runner=cast('LocalRunner', grizzly.state.locust),
                testdata={grizzly.scenario.class_name: {'AtomicIntegerIncrementer.foobar': i}},
            )
            grizzly.state.producer.has_persisted = True

            with caplog.at_level(logging.DEBUG):
                grizzly.state.producer.persist_data()

            assert caplog.messages == []
            assert not persistent_file.exists()

            grizzly.state.producer.has_persisted = False
            grizzly.state.producer.keystore = {'hello': 'world'}

            mocker.patch('grizzly.testdata.communication.jsondumps', side_effect=[json.JSONDecodeError])

            with caplog.at_level(logging.ERROR):
                grizzly.state.producer.persist_data()

            assert caplog.messages == ['failed to persist feature file data']
            assert not persistent_file.exists()

            caplog.clear()
        finally:
            cleanup()
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']


class TestTestdataConsumer:
    def test_testdata(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        def mock_testdata(consumer: TestdataConsumer, data: StrDict, action: str | None = 'consume') -> MagicMock:
            def send_message_mock(*_args: Any, **_kwargs: Any) -> None:
                message = Message(
                    'consume_testdata',
                    {
                        'uid': id(parent.user),
                        'cid': cast('LocalRunner', grizzly.state.locust).client_id,
                        'response': {'action': action, 'data': data},
                    },
                    node_id=None,
                )
                TestdataConsumer.handle_response(environment=consumer.runner.environment, msg=message)

            return mocker.patch.object(grizzly.state.locust, 'send_message', side_effect=send_message_mock)

        testdata_request_spy = mocker.spy(grizzly.events.testdata_request, 'fire')

        consumer = TestdataConsumer(cast('LocalRunner', grizzly.state.locust), parent)

        assert isinstance(consumer.async_timers, AsyncTimersConsumer)
        assert consumer.async_timers.on_report_to_master not in grizzly.state.locust.environment.events.report_to_master._handlers

        send_message = mock_testdata(
            consumer,
            {
                'auth.user.username': 'username',
                'auth.user.password': 'password',
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                },
            },
        )

        assert consumer.testdata() == {
            'auth': {
                'user': {
                    'username': 'username',
                    'password': 'password',
                },
            },
            'variables': transform(
                grizzly.scenario,
                {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                },
            ),
        }

        send_message.assert_called_once_with(
            'produce_testdata',
            {
                'uid': id(parent.user),
                'cid': cast('LocalRunner', grizzly.state.locust).client_id,
                'rid': ANYUUID(version=4),
                'request': {'message': 'testdata', 'identifier': 'TestScenario_001'},
            },
        )
        send_message.reset_mock()

        testdata_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'type': 'consumer',
                'action': 'consume',
                'identifier': consumer.identifier,
            },
            measurement='request_testdata',
            metrics={
                'error': None,
                'response_time': ANY(float),
            },
        )
        testdata_request_spy.reset_mock()

        send_message = mock_testdata(
            consumer,
            {
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                },
            },
            'stop',
        )

        with caplog.at_level(logging.DEBUG):
            assert consumer.testdata() is None
        assert caplog.messages[-1] == 'received stop command'

        send_message.assert_called_once_with(
            'produce_testdata',
            {
                'uid': id(parent.user),
                'cid': cast('LocalRunner', grizzly.state.locust).client_id,
                'rid': ANYUUID(version=4),
                'request': {'message': 'testdata', 'identifier': 'TestScenario_001'},
            },
        )
        send_message.reset_mock()

        caplog.clear()

        send_message = mock_testdata(
            consumer,
            {
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                },
            },
            'asdf',
        )

        with caplog.at_level(logging.DEBUG), pytest.raises(StopUser):
            consumer.testdata()
        assert 'unknown action "asdf" received, stopping user' in caplog.text

        send_message.assert_called_once_with(
            'produce_testdata',
            {
                'uid': id(parent.user),
                'cid': cast('LocalRunner', grizzly.state.locust).client_id,
                'rid': ANYUUID(version=4),
                'request': {'message': 'testdata', 'identifier': 'TestScenario_001'},
            },
        )
        send_message.reset_mock()

        caplog.clear()

        send_message = mock_testdata(
            consumer,
            {
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': None,
                },
            },
            'consume',
        )

        assert consumer.testdata() == {
            'variables': transform(
                grizzly.scenario,
                {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': None,
                },
            ),
        }

        send_message.assert_called_once_with(
            'produce_testdata',
            {
                'uid': id(parent.user),
                'cid': cast('LocalRunner', grizzly.state.locust).client_id,
                'rid': ANYUUID(version=4),
                'request': {'message': 'testdata', 'identifier': 'TestScenario_001'},
            },
        )
        send_message.reset_mock()

    @pytest.mark.parametrize('remove', [False, True])
    def test_keystore_get(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, remove: bool) -> None:  # noqa: FBT001
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast('LocalRunner', grizzly.state.locust), parent)

        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)

        assert consumer.keystore_get('hello', remove=remove) is None
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'get',
                'key': 'hello',
                'remove': remove,
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )
        keystore_request_spy.reset_mock()

        request_spy.assert_called_once_with(
            {
                'action': 'get',
                'key': 'hello',
                'remove': remove,
                'message': 'keystore',
                'identifier': consumer.identifier,
            },
        )

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo_add_data({'hello': 'world'}))

        assert consumer.keystore_get('hello', remove=remove) == {'hello': 'world'}
        request_spy.assert_called_once_with(
            {
                'action': 'get',
                'key': 'hello',
                'remove': remove,
                'message': 'keystore',
                'identifier': consumer.identifier,
            },
        )

    def test_keystore_set(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast('LocalRunner', grizzly.state.locust), parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        consumer.keystore_set('world', {'hello': 'world'})

        request_spy.assert_called_once_with(
            {
                'message': 'keystore',
                'action': 'set',
                'key': 'world',
                'identifier': consumer.identifier,
                'data': {'hello': 'world'},
            },
        )
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'set',
                'key': 'world',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )
        keystore_request_spy.reset_mock()

    def test_keystore_inc(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast('LocalRunner', grizzly.state.locust), parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        assert consumer.keystore_inc('counter') == 1

        request_spy.assert_called_once_with(
            {
                'action': 'inc',
                'key': 'counter',
                'message': 'keystore',
                'identifier': consumer.identifier,
                'data': 1,
            },
        )
        request_spy.reset_mock()
        keystore_request_spy.reset_mock()

        assert consumer.keystore_inc('counter', step=10) == 10

        request_spy.assert_called_once_with(
            {
                'action': 'inc',
                'key': 'counter',
                'message': 'keystore',
                'identifier': consumer.identifier,
                'data': 10,
            },
        )
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'inc',
                'key': 'counter',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )
        keystore_request_spy.reset_mock()

    def test_keystore_dec(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast('LocalRunner', grizzly.state.locust), parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        assert consumer.keystore_dec('counter') == 1

        request_spy.assert_called_once_with(
            {
                'action': 'dec',
                'key': 'counter',
                'message': 'keystore',
                'identifier': consumer.identifier,
                'data': 1,
            },
        )
        request_spy.reset_mock()
        keystore_request_spy.reset_mock()

        assert consumer.keystore_dec('counter', step=10) == 10

        request_spy.assert_called_once_with(
            {
                'action': 'dec',
                'key': 'counter',
                'message': 'keystore',
                'identifier': consumer.identifier,
                'data': 10,
            },
        )
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'dec',
                'key': 'counter',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )
        keystore_request_spy.reset_mock()

    def test_keystore_push(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast('LocalRunner', grizzly.state.locust), parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        consumer.keystore_push('foobar', 'hello')

        request_spy.assert_called_once_with(
            {
                'action': 'push',
                'key': 'foobar',
                'message': 'keystore',
                'identifier': consumer.identifier,
                'data': 'hello',
            },
        )
        request_spy.reset_mock()
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'push',
                'key': 'foobar',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )

    def test_keystore_pop(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast('LocalRunner', grizzly.state.locust), parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo_add_data([None, None, 'hello']))
        gsleep_mock = mocker.patch('grizzly.testdata.communication.gsleep', return_value=None)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        assert consumer.keystore_pop('foobar') == 'hello'

        assert gsleep_mock.call_count == 2
        assert request_spy.call_count == 3
        assert keystore_request_spy.call_count == 3

    def test_keystore_del(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast('LocalRunner', grizzly.state.locust), parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        consumer.keystore_del('foobar')

        request_spy.assert_called_once_with(
            {
                'action': 'del',
                'key': 'foobar',
                'message': 'keystore',
                'identifier': consumer.identifier,
            },
        )
        request_spy.reset_mock()
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'del',
                'key': 'foobar',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )
