from typing import Callable, cast
from datetime import datetime

import pytest

from behave.runner import Context
from grizzly.context import GrizzlyContext
from grizzly.task import DateTask

from ..fixtures import grizzly_context, request_task, behave_context, locust_environment  # pylint: disable=unused-import

class TestDateTask:
    def test___init__(self) -> None:
        with pytest.raises(ValueError) as ve:
            DateTask('date_variable', '2022-01-17')
        assert 'no arguments specified' in str(ve)

        with pytest.raises(ValueError) as ve:
            DateTask('date_variable', '2022-01-17 | asdf=True')
        assert 'unsupported arguments asdf' in str(ve)

        task = DateTask('date_variable', '{{ datetime.now() }} | offset=-1D, timezone=UTC, format="%Y-%m-%d"')

        assert task.value == '{{ datetime.now() }}'
        assert task.arguments.get('offset', None) == '-1D'
        assert task.arguments.get('timezone', None) == 'UTC'
        assert task.arguments.get('format', None) == '%Y-%m-%d'

    @pytest.mark.usefixtures('behave_context', 'grizzly_context')
    def test_implementation(self, behave_context: Context, grizzly_context: Callable) -> None:
        grizzly = cast(GrizzlyContext, behave_context.grizzly)
        grizzly.state.variables.update({'date_variable': 'none'})

        _, _, tasks, _ = grizzly_context()

        task = DateTask('date_variable', '2022-01-17 10:37:01 | offset=-1D')
        implementation = task.implementation()
        implementation(tasks)

        assert tasks.user._context['variables']['date_variable'] == '2022-01-16 10:37:01'

        task = DateTask('date_variable', '2022-01-17 10:37:01 | offset=-22Y-1D, timezone=UTC')
        implementation = task.implementation()
        implementation(tasks)

        assert tasks.user._context['variables']['date_variable'] == '2000-01-16 09:37:01'

        task = DateTask('date_variable', '2022-01-17 10:37:01 | offset=-22Y-16D-37m59s, timezone=UTC, format="%-d/%-m %y %H.%M.%S"')
        implementation = task.implementation()
        implementation(tasks)

        assert tasks.user._context['variables']['date_variable'] == '1/1 00 09.01.00'

        expected = datetime.now().strftime('%-d/%-m -%y')
        task = DateTask('date_variable', '{{ datetime.now() }} | format="%-d/%-m -%y"')
        implementation = task.implementation()
        implementation(tasks)

        assert tasks.user._context['variables']['date_variable'] == expected

        tasks.user._context['variables']['date_value'] = '2022-01-17T10:48:37.000'
        task = DateTask('date_variable', '{{ date_value }} | timezone=UTC, offset=-22Y2M3D, format="%-d/%-m %y %H:%M:%S"')
        implementation = task.implementation()
        implementation(tasks)

        assert tasks.user._context['variables']['date_variable'] == '20/3 00 09:48:37'

        task.arguments['offset'] = 'asdf'

        with pytest.raises(ValueError) as ve:
            implementation(tasks)
        assert 'invalid time span format' in str(ve)

        task.arguments.update({'offset': None, 'timezone': 'DisneyWorld/Ankeborg'})

        with pytest.raises(ValueError) as ve:
            implementation(tasks)
        assert '"DisneyWorld/Ankeborg" is not a valid time zone' in str(ve)

        task = DateTask('date_variable', 'asdf | timezone=UTC, offset=-22Y2M3D, format="%-d/%-m %y %H:%M:%S"')
        implementation = task.implementation()

        with pytest.raises(ValueError) as ve:
            implementation(tasks)
        assert '"asdf" is not a valid datetime string' in str(ve)

        task = DateTask('date_variable', '{{ datetime.now().strftime("%Y") }} | timezone=UTC, format=%Y')
        implementation = task.implementation()

        implementation(tasks)

        assert tasks.user._context['variables']['date_variable'] == datetime.now().strftime('%Y')
