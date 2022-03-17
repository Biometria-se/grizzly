from typing import cast
from datetime import datetime

import pytest

from grizzly.context import GrizzlyContext
from grizzly.tasks import DateTask

from ..fixtures import GrizzlyFixture


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

    def test_implementation(self, grizzly_fixture: GrizzlyFixture) -> None:
        behave = grizzly_fixture.behave
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.state.variables.update({'date_variable': 'none'})

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        task = DateTask('date_variable', '2022-01-17 10:37:01 | offset=-1D')
        implementation = task.implementation()
        implementation(scenario)

        assert scenario.user._context['variables']['date_variable'] == '2022-01-16 10:37:01'

        task = DateTask('date_variable', '2022-01-17 10:37:01 | offset=-22Y-1D, timezone=UTC')
        implementation = task.implementation()
        implementation(scenario)

        assert scenario.user._context['variables']['date_variable'] == '2000-01-16 09:37:01'

        task = DateTask('date_variable', '2022-01-17 10:37:01 | offset=-22Y-16D-37m59s, timezone=UTC, format="%-d/%-m %y %H.%M.%S"')
        implementation = task.implementation()
        implementation(scenario)

        assert scenario.user._context['variables']['date_variable'] == '1/1 00 09.01.00'

        expected = datetime.now().strftime('%-d/%-m -%y')
        task = DateTask('date_variable', '{{ datetime.now() }} | format="%-d/%-m -%y"')
        implementation = task.implementation()
        implementation(scenario)

        assert scenario.user._context['variables']['date_variable'] == expected

        scenario.user._context['variables']['date_value'] = '2022-01-17T10:48:37.000'
        task = DateTask('date_variable', '{{ date_value }} | timezone=UTC, offset=-22Y2M3D, format="%-d/%-m %y %H:%M:%S"')
        implementation = task.implementation()
        implementation(scenario)

        assert scenario.user._context['variables']['date_variable'] == '20/3 00 09:48:37'

        task.arguments['offset'] = 'asdf'

        with pytest.raises(ValueError) as ve:
            implementation(scenario)
        assert 'invalid time span format' in str(ve)

        task.arguments.update({'offset': None, 'timezone': 'DisneyWorld/Ankeborg'})

        with pytest.raises(ValueError) as ve:
            implementation(scenario)
        assert '"DisneyWorld/Ankeborg" is not a valid time zone' in str(ve)

        task = DateTask('date_variable', 'asdf | timezone=UTC, offset=-22Y2M3D, format="%-d/%-m %y %H:%M:%S"')
        implementation = task.implementation()

        with pytest.raises(ValueError) as ve:
            implementation(scenario)
        assert '"asdf" is not a valid datetime string' in str(ve)

        task = DateTask('date_variable', '{{ datetime.now().strftime("%Y") }} | timezone=UTC, format=%Y')
        implementation = task.implementation()

        implementation(scenario)

        assert scenario.user._context['variables']['date_variable'] == datetime.now().strftime('%Y')

        scenario.user._context['variables'].update({
            'to_year': '2022',
            'to_month': '01',
            'to_day': '18',
        })

        task = DateTask('date_variable', '{{ to_year }}-{{ to_month }}-{{ to_day }} | format="%Y", offset=-1D')
        implementation = task.implementation()

        implementation(scenario)

        assert scenario.user._context['variables']['date_variable'] == '2022'
