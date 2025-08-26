"""Unit tests of grizzly.steps.scenario.tasks.transformer."""

from __future__ import annotations

from json import dumps as jsondumps
from typing import TYPE_CHECKING, cast

from grizzly.steps import step_task_transformer_parse
from grizzly.tasks import TransformerTask
from grizzly_common.transformer import TransformerContentType

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture


def test_step_task_transformer_parse_json(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_transformer_parse(
        behave,
        jsondumps(
            {
                'document': {
                    'id': 'DOCUMENT_8483-1',
                    'title': 'TPM Report 2020',
                },
            },
        ),
        TransformerContentType.JSON,
        '$.document.id',
        'document_id',
    )
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='TransformerTask: document_id has not been initialized')]}
    behave.exceptions.clear()

    grizzly.scenario.variables['document_id'] = 'None'
    step_task_transformer_parse(
        behave,
        jsondumps(
            {
                'document': {
                    'id': 'DOCUMENT_8483-1',
                    'title': 'TPM Report 2020',
                },
            },
        ),
        TransformerContentType.JSON,
        '$.document.id',
        'document_id',
    )

    assert behave.exceptions == {}

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, TransformerTask)
    assert task.content_type == TransformerContentType.JSON
    assert task.expression == '$.document.id'
    assert task.variable == 'document_id'

    assert len(grizzly.scenario.orphan_templates) == 0

    step_task_transformer_parse(
        behave,
        jsondumps(
            {
                'document': {
                    'id': 'DOCUMENT_8483-1',
                    'title': 'TPM Report {{ year }}',
                },
            },
        ),
        TransformerContentType.JSON,
        '$.document.id',
        'document_id',
    )

    templates = grizzly.scenario.tasks()[-1].get_templates()

    assert len(templates) == 1
    assert templates[-1] == jsondumps(
        {
            'document': {
                'id': 'DOCUMENT_8483-1',
                'title': 'TPM Report {{ year }}',
            },
        },
    )


def test_step_task_transformer_parse_xml(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_transformer_parse(
        behave,
        """<?xml version="1.0" encoding="utf-8"?>
<document>
    <id>DOCUMENT_8483-1</id>
    <title>TPM Report 2022</title>
</document>
        """,
        TransformerContentType.XML,
        '/document/id/text()',
        'document_id',
    )
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='TransformerTask: document_id has not been initialized')]}

    grizzly.scenario.variables['document_id'] = 'None'
    step_task_transformer_parse(
        behave,
        """<?xml version="1.0" encoding="utf-8"?>
<document>
    <id>DOCUMENT_8483-1</id>
    <title>TPM Report 2022</title>
</document>
        """,
        TransformerContentType.XML,
        '/document/id/text()',
        'document_id',
    )

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, TransformerTask)
    assert task.content_type == TransformerContentType.XML
    assert task.expression == '/document/id/text()'
    assert task.variable == 'document_id'

    assert len(grizzly.scenario.orphan_templates) == 0

    step_task_transformer_parse(
        behave,
        """<?xml version="1.0" encoding="utf-8"?>
<document>
    <id>DOCUMENT_8483-1</id>
    <title>TPM Report {{ year }}</title>
</document>
        """,
        TransformerContentType.XML,
        '/document/id/text()',
        'document_id',
    )

    templates = grizzly.scenario.tasks()[-1].get_templates()

    assert len(templates) == 1
    assert (
        templates[-1]
        == """<?xml version="1.0" encoding="utf-8"?>
<document>
    <id>DOCUMENT_8483-1</id>
    <title>TPM Report {{ year }}</title>
</document>
        """
    )
