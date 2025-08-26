from __future__ import annotations

from itertools import product
from typing import TYPE_CHECKING, Any

from grizzly_ls.model import Step
from grizzly_ls.server.features.diagnostics import GrizzlyDiagnostic, validate_gherkin
from lsprotocol import types as lsp
from pygls.workspace import TextDocument

from test_ls.helpers import SOME

if TYPE_CHECKING:
    from test_ls.fixtures import LspFixture


def test_validate_gherkin(lsp_fixture: LspFixture) -> None:
    ls = lsp_fixture.server

    ls.language = 'en'

    # <!-- no language yet
    text_document = TextDocument(
        'file://test.feature',
        """# language:
Feature:
    Scenario: test
""",
    )
    diagnostics = validate_gherkin(ls, text_document)

    assert diagnostics == []
    # // -->

    # <!-- language invalid + wrong line
    text_document = TextDocument(
        'file://test.feature',
        '''
# language: asdf
Feature:
    """
    this is just a comment
    """
    Scenario: test
''',
    )
    diagnostics = validate_gherkin(ls, text_document)

    assert [
        SOME(  # wrong language
            lsp.Diagnostic,
            range=lsp.Range(
                start=lsp.Position(line=1, character=12),
                end=lsp.Position(line=1, character=16),
            ),
            message='"asdf" is not a valid language',
            severity=lsp.DiagnosticSeverity.Error,
            code=None,
            code_description=None,
            source=ls.__class__.__name__,
            tags=None,
            related_information=None,
            data=None,
        ),
        SOME(  # wrong line
            lsp.Diagnostic,
            range=lsp.Range(
                start=lsp.Position(line=1, character=0),
                end=lsp.Position(line=1, character=16),
            ),
            message='"# language:" should be on the first line',
            severity=lsp.DiagnosticSeverity.Warning,
            code=None,
            code_description=None,
            source=ls.__class__.__name__,
            tags=None,
            related_information=None,
            data=None,
        ),
    ] == diagnostics
    # // -->

    # <!-- keyword language != specified language
    ls.language = 'sv'
    text_document = TextDocument(
        'file://test.feature',
        '''# language: sv
Feature:
    """
    this is just a comment
    """
    Scenario: test
''',
    )
    diagnostics = validate_gherkin(ls, text_document)

    assert [
        SOME(
            lsp.Diagnostic,
            range=lsp.Range(
                start=lsp.Position(line=1, character=0),
                end=lsp.Position(line=1, character=7),
            ),
            message='"Feature" is not a valid keyword in Swedish',
            severity=lsp.DiagnosticSeverity.Error,
            code=None,
            code_description=None,
            source=ls.__class__.__name__,
            tags=None,
            related_information=None,
            data=None,
        ),
        SOME(
            lsp.Diagnostic,
            range=lsp.Range(
                start=lsp.Position(line=2, character=0),
                end=lsp.Position(line=2, character=7),
            ),
            message='Parser failure in state initial\nNo feature found.',
            severity=lsp.DiagnosticSeverity.Error,
            code=None,
            code_description=None,
            source=ls.__class__.__name__,
            tags=None,
            related_information=None,
            data=None,
        ),
    ] == diagnostics
    # // -->

    # <!-- step implementation not found
    ls.language = 'en'
    text_document = TextDocument(
        'file://test.feature',
        '''# language: en
Feature:
    """
    this is just a comment
    """
    Scenario: test
        Given a step in the scenario
        And another expression with a "variable"

        Then this step actually exists!
''',
    )

    def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    ls.steps.update({'then': [Step('then', 'this step actually exists!', func=noop)]})
    diagnostics = validate_gherkin(ls, text_document)
    assert [
        SOME(
            lsp.Diagnostic,
            range=lsp.Range(
                start=lsp.Position(line=6, character=14),
                end=lsp.Position(line=6, character=36),
            ),
            message='No step implementation found\nGiven a step in the scenario',
            severity=lsp.DiagnosticSeverity.Warning,
            code=None,
            code_description=None,
            source=ls.__class__.__name__,
            tags=None,
            related_information=None,
            data=None,
        ),
        SOME(
            lsp.Diagnostic,
            range=lsp.Range(
                start=lsp.Position(line=7, character=12),
                end=lsp.Position(line=7, character=48),
            ),
            message='No step implementation found\nAnd another expression with a "variable"',
            severity=lsp.DiagnosticSeverity.Warning,
            code=None,
            code_description=None,
            source=ls.__class__.__name__,
            tags=None,
            related_information=None,
            data=None,
        ),
    ] == diagnostics
    # // -->

    # <!-- freetext marker not closed
    ls.language = 'en'
    text_document = TextDocument(
        'file://test.feature',
        '''# language: en
Feature:
    """
    this is just a comment
    Scenario: test
        Then this step actually exists!
''',
    )

    diagnostics = validate_gherkin(ls, text_document)

    assert [SOME(lsp.Diagnostic, message='Freetext marker is not closed', severity=lsp.DiagnosticSeverity.Error)] == diagnostics
    # // -->

    # <!-- "complex" document with no errors
    feature_file = lsp_fixture.datadir / 'features' / 'test.feature'
    included_feature_file_1 = lsp_fixture.datadir / 'features' / 'hello.feature'
    included_feature_file_2 = lsp_fixture.datadir / 'world.feature'

    feature_file.write_text(
        '''# language: sv
    # testspecifikation: https://test.nu/specifikation/T01
    Egenskap: T01
        """
        lite text
        bara
        """
        Scenario: test
            Givet en tabell
            # denna tabell mappar en nyckel med ett värde
            | nyckel | värde |
            | foo    | bar   |
            | bar    | foo   |

            Och följande fråga
            """
            SELECT key, value FROM [dbo].[tests]
            """

            Så producera ett dokument i formatet "json"

        Scenario: inkluderat-1
            {% scenario "hello" feature="./hello.feature" %}

        Scenario: inkluderat-2
            {% scenario "world" "./hello.feature" %}

        Scenario: inkluderat-3
            {% scenario scenario="foo", feature="./hello.feature" %}

        Scenario: inkluderat-4
            {%  scenario  scenario="bar" ,   "./hello.feature" %}

        # Scenario: inactive
        #   {% scenario "hello", feature="../world.feature" %}

        Scenario: inkluderat-5
            {% scenario "world", feature="../world.feature" %}
    ''',
        encoding='utf-8',
    )

    included_feature_file_1.write_text(
        """# language: sv
Egenskap: hello
    Scenario: hello
        Så producera ett dokument i formatet "xml"

    Scenario: world
        Så producera ett dokument i formatet "yaml"

    Scenario: foo
        Så producera en bild i formatet "gif"

    Scenario: bar
        Så producera en bild i formatet "png"
    """,
        encoding='utf-8',
    )

    included_feature_file_2.write_text(
        """# language: sv
Egenskap: hello
    Scenario: hello
        Så producera ett dokument i formatet "xml"

    Scenario: world
        Så producera ett dokument i formatet "yaml"
    """,
        encoding='utf-8',
    )

    try:
        ls.language = 'sv'
        text_document = TextDocument(feature_file.as_uri())

        ls.steps.update(
            {
                'then': [
                    Step('then', 'producera ett dokument i formatet "json"', func=noop),
                    Step('then', 'producera ett dokument i formatet "xml"', func=noop),
                    Step('then', 'producera ett dokument i formatet "docx"', func=noop),
                ],
                'given': [Step('given', 'en tabell', func=noop)],
                'step': [Step('step', 'följande fråga', func=noop)],
            }
        )
        diagnostics = validate_gherkin(ls, text_document)

        assert diagnostics == []
    finally:
        ls.language = 'en'
        feature_file.unlink()
        included_feature_file_1.unlink()
        included_feature_file_2.unlink()
    # // -->


def test_validate_gherkin_scenario_tag(lsp_fixture: LspFixture) -> None:
    ls = lsp_fixture.server

    feature_file = lsp_fixture.datadir / 'features' / 'test_validate_gherkin_scenario_tag.feature'
    included_feature_file = lsp_fixture.datadir / 'features' / 'test_validate_gherkin_scenario_tag_include.feature'

    try:
        # <!-- jinja2 expression, not scenario tag -- ignored
        feature_file.write_text(
            """Feature: test scenario tag
    Scenario: included
        {% hello %}
    """,
            encoding='utf-8',
        )
        text_document = TextDocument(feature_file.as_posix())

        assert validate_gherkin(ls, text_document) == []
        # // -->

        # <!-- scenario tag, no arguments
        feature_file.write_text(
            """Feature: test scenario tag
    Scenario: included
        {% scenario %}
    """,
            encoding='utf-8',
        )
        text_document = TextDocument(feature_file.as_posix())

        assert [
            SOME(
                GrizzlyDiagnostic,
                message='Scenario tag is invalid, could not find scenario argument',
                range=lsp.Range(start=lsp.Position(line=2, character=8), end=lsp.Position(line=2, character=22)),
                severity=lsp.DiagnosticSeverity.Error,
            ),
            SOME(
                GrizzlyDiagnostic,
                message='Scenario tag is invalid, could not find feature argument',
                range=lsp.Range(start=lsp.Position(line=2, character=8), end=lsp.Position(line=2, character=22)),
                severity=lsp.DiagnosticSeverity.Error,
            ),
        ] == validate_gherkin(ls, text_document)
        # // -->

        # <!-- empty scenario and feature arguments
        feature_file.write_text(
            """Feature: test scenario tag
    Scenario: included
        {% scenario "", feature="" %}
    """,
            encoding='utf-8',
        )
        text_document = TextDocument(feature_file.as_posix())

        assert [
            SOME(
                GrizzlyDiagnostic,
                message='Feature argument is empty',
                range=lsp.Range(start=lsp.Position(line=2, character=33), end=lsp.Position(line=2, character=33)),
                severity=lsp.DiagnosticSeverity.Warning,
            ),
            SOME(
                GrizzlyDiagnostic,
                message='Scenario argument is empty',
                range=lsp.Range(start=lsp.Position(line=2, character=21), end=lsp.Position(line=2, character=21)),
                severity=lsp.DiagnosticSeverity.Warning,
            ),
        ] == validate_gherkin(ls, text_document)
        # // -->

        # <!-- missing feature argument, scenario argument both as positional and named
        for argument in ['"foo"', 'scenario="foo"']:
            feature_file.write_text(
                f"""Feature: test scenario tag
    Scenario: included
        {{% scenario {argument} %}}
    """,
                encoding='utf-8',
            )
            text_document = TextDocument(feature_file.as_posix())

            assert [
                SOME(
                    lsp.Diagnostic,
                    message='Scenario tag is invalid, could not find feature argument',
                    range=lsp.Range(start=lsp.Position(line=2, character=8), end=lsp.Position(line=2, character=len(argument) + 21 + 2)),
                    severity=lsp.DiagnosticSeverity.Error,
                ),
            ] == validate_gherkin(ls, text_document)
        # // -->

        # <!-- specified feature file that does not exist
        for arg_scenario, arg_feature in product(
            ['"foo"', 'scenario="foo"'],
            [
                '"./test_validate_gherkin_scenario_tag_include.feature"',
                'feature="./test_validate_gherkin_scenario_tag_include.feature"',
            ],
        ):
            for prefix in [None, (lsp_fixture.datadir / 'features').as_posix()]:
                if prefix is not None:
                    arg_feature = arg_feature.replace('./', f'{prefix}/')  # noqa: PLW2901

                feature_file.write_text(
                    f"""Feature: test scenario tag
        Scenario: included
            {{% scenario {arg_scenario}, {arg_feature} %}}
        """,
                    encoding='utf-8',
                )
                text_document = TextDocument(feature_file.as_posix())

                included_feature_file.unlink(missing_ok=True)
                _, feature_file_name, _ = arg_feature.split('"', 3)

                assert [SOME(lsp.Diagnostic, message=f'Included feature file "{feature_file_name}" does not exist')] == validate_gherkin(ls, text_document)

                included_feature_file.touch()

                assert [SOME(lsp.Diagnostic, message=f'Included feature file "{feature_file_name}" does not have any scenarios')] == validate_gherkin(ls, text_document)

                included_feature_file.write_text("""Egenskap: test""", encoding='utf-8')

                assert [SOME(lsp.Diagnostic, message='Parser failure in state initial\nNo feature found.')] == validate_gherkin(ls, text_document)

                included_feature_file.write_text("""Feature: test""", encoding='utf-8')

                assert [SOME(lsp.Diagnostic, message=f'Scenario "foo" does not exist in included feature "{feature_file_name}"')] == validate_gherkin(ls, text_document)

                included_feature_file.write_text(
                    """Feature: test
Scenario: foo""",
                    encoding='utf-8',
                )

                assert [SOME(lsp.Diagnostic, message=f'Scenario "foo" in "{feature_file_name}" does not have any steps')] == validate_gherkin(ls, text_document)

                included_feature_file.write_text(
                    """Feature: test
Scenario: foo
    Given a step expression""",
                    encoding='utf-8',
                )

                assert validate_gherkin(ls, text_document) == []
        # // -->

        # <!-- scenario tag values argument
        feature_file.write_text(
            """Feature: test scenario tag
    Scenario: included
        {% scenario "include", feature="./test_validate_gherkin_scenario_tag_include.feature", foo="bar", bar="foo" %}
    """,
            encoding='utf-8',
        )

        included_feature_file = lsp_fixture.datadir / 'features' / 'test_validate_gherkin_scenario_tag_include.feature'
        included_feature_file.write_text(
            """Feature:
    Scenario: include
        Given a step expression
    """,
            encoding='utf-8',
        )
        text_document = TextDocument(feature_file.as_posix())

        assert [
            SOME(
                lsp.Diagnostic,
                message='Declared variable "foo" is not used in included scenario steps',
                range=lsp.Range(start=lsp.Position(line=2, character=95), end=lsp.Position(line=2, character=104)),
                severity=lsp.DiagnosticSeverity.Error,
            ),
            SOME(
                lsp.Diagnostic,
                message='Declared variable "bar" is not used in included scenario steps',
                range=lsp.Range(start=lsp.Position(line=2, character=106), end=lsp.Position(line=2, character=115)),
                severity=lsp.DiagnosticSeverity.Error,
            ),
        ] == validate_gherkin(ls, text_document)

        included_feature_file.write_text(
            """Feature:
    Scenario: include
        Given a step expression named "{$ foo $}"
        And a step expression named "{$ baz $}"

""",
            encoding='utf-8',
        )
        text_document = TextDocument(feature_file.as_posix())

        assert [
            SOME(
                lsp.Diagnostic,
                message='Declared variable "bar" is not used in included scenario steps',
                range=lsp.Range(start=lsp.Position(line=2, character=106), end=lsp.Position(line=2, character=115)),
                severity=lsp.DiagnosticSeverity.Error,
            ),
            SOME(
                lsp.Diagnostic,
                message='Scenario tag is missing variable "baz"',
                range=lsp.Range(start=lsp.Position(line=2, character=8), end=lsp.Position(line=2, character=118)),
                severity=lsp.DiagnosticSeverity.Warning,
            ),
        ] == validate_gherkin(ls, text_document)

        included_feature_file.write_text(
            """Feature:
    Scenario: include
        Given a step expression named "{$ foo $}"
        And a step expression named "{$ bar $}"

""",
            encoding='utf-8',
        )
        text_document = TextDocument(feature_file.as_posix())

        assert validate_gherkin(ls, text_document) == []
        # // -->
    finally:
        included_feature_file.unlink(missing_ok=True)
        feature_file.unlink(missing_ok=True)
