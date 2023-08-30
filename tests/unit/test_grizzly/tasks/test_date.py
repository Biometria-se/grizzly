from typing import cast
from datetime import datetime

import pytest

from pytest_mock import MockerFixture
from dateutil.parser import parse as dateparser

from grizzly.context import GrizzlyContext
from grizzly.tasks import DateTask

from tests.fixtures import GrizzlyFixture


class TestDateTask:
    def test___init__(self) -> None:
        with pytest.raises(ValueError) as ve:
            DateTask('date_variable', '2022-01-17')
        assert 'no arguments specified' in str(ve)

        with pytest.raises(ValueError) as ve:
            DateTask('date_variable', '2022-01-17 | asdf=True')
        assert 'unsupported arguments asdf' in str(ve)

        task_factory = DateTask('date_variable', '{{ datetime.now() }} | offset=-1D, timezone="{{ timezone }}", format="%Y-%m-%d"')

        assert task_factory.value == '{{ datetime.now() }}'
        assert task_factory.arguments.get('offset', None) == '-1D'
        assert task_factory.arguments.get('timezone', None) == '{{ timezone }}'
        assert task_factory.arguments.get('format', None) == '%Y-%m-%d'
        assert task_factory.__template_attributes__ == {'value', 'arguments'}
        templates = sorted(task_factory.get_templates())
        assert len(templates) == 2
        assert templates == sorted([
            '{{ datetime.now() }}',
            '{{ timezone }}',
        ])

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        behave = grizzly_fixture.behave.context
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.state.variables.update({'date_variable': 'none'})

        parent = grizzly_fixture()

        assert parent is not None

        task_factory = DateTask('date_variable', '2022-01-17 10:37:01 | offset=-1D')
        task = task_factory()
        task(parent)

        assert parent.user._context['variables']['date_variable'] == '2022-01-16 10:37:01'

        task_factory = DateTask('date_variable', '2022-01-17 10:37:01 | offset=-22Y-1D, timezone=UTC')
        task = task_factory()
        task(parent)

        assert parent.user._context['variables']['date_variable'] == '2000-01-16 09:37:01'

        task_factory = DateTask('date_variable', '2022-01-17 10:37:01 | offset=-22Y-16D-37m59s, timezone=UTC, format="%d/%m %y %H.%M.%S"')
        task = task_factory()
        task(parent)

        assert parent.user._context['variables']['date_variable'] == '01/01 00 09.01.00'

        expected = datetime.now().strftime('%d/%m -%y')
        task_factory = DateTask('date_variable', '{{ datetime.now() }} | format="%d/%m -%y"')
        task = task_factory()
        task(parent)

        assert parent.user._context['variables']['date_variable'] == expected

        parent.user._context['variables']['date_value'] = '2022-01-17T10:48:37.000'
        task_factory = DateTask('date_variable', '{{ date_value }} | timezone=UTC, offset=-22Y2M3D, format="%d/%m %y %H:%M:%S"')
        task = task_factory()
        task(parent)

        assert parent.user._context['variables']['date_variable'] == '20/03 00 09:48:37'

        task_factory.arguments['offset'] = 'asdf'

        with pytest.raises(ValueError) as ve:
            task(parent)
        assert 'invalid time span format' in str(ve)

        task_factory.arguments.update({'offset': None, 'timezone': 'DisneyWorld/Ankeborg'})

        with pytest.raises(ValueError) as ve:
            task(parent)
        assert '"DisneyWorld/Ankeborg" is not a valid time zone' in str(ve)

        task_factory = DateTask('date_variable', 'asdf | timezone=UTC, offset=-22Y2M3D, format="%-d/%-m %y %H:%M:%S"')
        task = task_factory()

        with pytest.raises(ValueError) as ve:
            task(parent)
        assert '"asdf" is not a valid datetime string' in str(ve)

        task_factory = DateTask('date_variable', '{{ datetime.now().strftime("%Y") }} | timezone=UTC, format=%Y')
        task = task_factory()

        task(parent)

        assert parent.user._context['variables']['date_variable'] == datetime.now().strftime('%Y')

        parent.user._context['variables'].update({
            'to_year': '2022',
            'to_month': '01',
            'to_day': '18',
        })

        task_factory = DateTask('date_variable', '{{ to_year }}-{{ to_month }}-{{ to_day }} | format="%Y", offset=-1D')
        task = task_factory()

        task(parent)

        assert parent.user._context['variables']['date_variable'] == '2022'

        expected_datetime = dateparser('2022-05-19 07:20:00.123456+0200')

        datetime_mock = mocker.patch(
            'grizzly.tasks.date.datetime',
            side_effect=lambda *args, **kwargs: datetime(*args, **kwargs)
        )
        datetime_mock.now.return_value = expected_datetime

        task_factory = DateTask('date_variable', '{{ datetime.now() }} | format="ISO-8601:DateTime"')
        task = task_factory()

        task(parent)

        assert parent.user._context['variables']['date_variable'] == '2022-05-19T07:20:00+02:00'

        task_factory = DateTask('date_variable', '{{ datetime.now() }} | format="ISO-8601:DateTime:ms"')
        task = task_factory()

        task(parent)

        assert parent.user._context['variables']['date_variable'] == '2022-05-19T07:20:00.123456+02:00'

        task_factory = DateTask('date_variable', '{{ datetime.now() }} | format="ISO-8601:Time"')
        task = task_factory()

        task(parent)

        expected = expected_datetime.replace(microsecond=0).isoformat()
        _, expected = expected.split('T', 1)

        assert parent.user._context['variables']['date_variable'] == '07:20:00+02:00'

        task_factory = DateTask('date_variable', '{{ datetime.now() }} | format="ISO-8601:Time:ms"')
        task = task_factory()

        task(parent)

        assert parent.user._context['variables']['date_variable'] == '07:20:00.123456+02:00'

        task_factory = DateTask('date_variable', '{{ datetime.now() }} | format="ISO-8601:DateTime:ms:no-sep"')
        task = task_factory()

        task(parent)

        assert parent.user._context['variables']['date_variable'] == '20220519T072000123456+02:00'

        task_factory = DateTask('date_variable', '{{ datetime.now() }} | format="ISO-8601:DateTime:no-sep"')
        task = task_factory()

        task(parent)

        assert parent.user._context['variables']['date_variable'] == '20220519T072000+02:00'
