from __future__ import annotations

import itertools
import logging
import re
from difflib import get_close_matches
from tokenize import NAME, OP
from typing import TYPE_CHECKING

from behave.i18n import languages
from lsprotocol import types as lsp

from grizzly_ls.constants import MARKER_LANGUAGE
from grizzly_ls.text import get_tokens

if TYPE_CHECKING:  # pragma: no cover
    from pygls.workspace import TextDocument

    from grizzly_ls.server import GrizzlyLanguageServer


logger = logging.getLogger(__name__)


def get_trigger(value: str, trigger: str) -> bool | str | None:
    partial_value: str | None = None

    tokens_reversed = list(reversed(get_tokens(value)))

    for index, next_token in enumerate(tokens_reversed):
        if index == 0 and next_token.type == NAME:
            partial_value = next_token.string
            continue

        try:
            token = tokens_reversed[index + 1]
            if token.type == OP and token.string == trigger[0] and next_token.type == OP and next_token.string == trigger[1]:
                return partial_value
        except IndexError:  # no variable name...
            continue

    return False


def complete_metadata(
    line: str,
    position: lsp.Position,
) -> list[lsp.CompletionItem]:
    items: list[lsp.CompletionItem] = []
    if line.startswith(MARKER_LANGUAGE):
        _, expression = line.strip().split(MARKER_LANGUAGE, 1)
        expression = expression.strip()

        for lang, localization in languages.items():
            name = localization.get('name', ['___12341234_asdf'])[0]
            native = localization.get('native', ['___12341234_asdf'])[0]
            if not (expression.lower() in name.lower() or expression.lower() in native.lower() or expression.lower() in lang) and len(expression.strip()) > 0:
                continue

            text_edit = lsp.TextEdit(
                range=lsp.Range(
                    start=lsp.Position(
                        line=position.line,
                        character=position.character - len(expression),
                    ),
                    end=position,
                ),
                new_text=lang,
            )

            items.append(
                lsp.CompletionItem(
                    label=lang,
                    kind=lsp.CompletionItemKind.Property,
                    text_edit=text_edit,
                )
            )
    else:
        text_edit = lsp.TextEdit(
            range=lsp.Range(
                start=lsp.Position(line=position.line, character=0),
                end=position,
            ),
            new_text=f'{MARKER_LANGUAGE} ',
        )
        items = [
            lsp.CompletionItem(
                label=MARKER_LANGUAGE,
                kind=lsp.CompletionItemKind.Property,
                text_edit=text_edit,
            )
        ]

    return items


def complete_keyword(
    ls: GrizzlyLanguageServer,
    keyword: str | None,
    position: lsp.Position,
    text_document: TextDocument,
) -> list[lsp.CompletionItem]:
    items: list[lsp.CompletionItem] = []
    if len(text_document.source.strip()) < 1:
        keywords = [*ls.localizations.get('feature', [])]
    else:
        scenario_keywords = [
            *ls.localizations.get('scenario', []),
            *ls.localizations.get('scenario_outline', []),
        ]

        keywords = scenario_keywords if not any(scenario_keyword in text_document.source for scenario_keyword in scenario_keywords) else ls.keywords.copy()

        for key in ['feature', 'background']:
            keys = ls.localizations.get(key, [])
            if any(f'{keyword_once}:' in text_document.source for keyword_once in keys):
                continue

            keywords.extend(keys)

        # check for partial matches
        if keyword is not None:
            keywords = [k for k in keywords if keyword.strip().lower() in k.lower()]

    for suggested_keyword in sorted(keywords):
        start = lsp.Position(line=position.line, character=position.character - len(keyword or ''))
        suffix = ': ' if suggested_keyword in ls.keywords_headers else ' '

        text_edit = lsp.TextEdit(
            range=lsp.Range(
                start=start,
                end=position,
            ),
            new_text=f'{suggested_keyword}{suffix}',
        )

        items.append(
            lsp.CompletionItem(
                label=suggested_keyword,
                kind=lsp.CompletionItemKind.Keyword,
                deprecated=False,
                text_edit=text_edit,
            )
        )

    return items


def complete_expression(
    _ls: GrizzlyLanguageServer,
    line: str,
    _text_document: TextDocument,
    position: lsp.Position,
    *,
    partial: str | None = None,
) -> list[lsp.CompletionItem]:
    start = lsp.Position(line=position.line, character=position.character - len(partial or ''))

    # ugly workaround for now, as to not insert complete text on already written text
    # that would result in an invalid expression...
    if (partial is not None and 'scenario' in partial) or '%}' in line:
        return []

    new_text = 'scenario "$1", feature="$2" %}'

    right_stripped_line = line.rstrip()

    # is there a whitespace or not when auto-complete triggered?
    if right_stripped_line == line and (partial is None or (not new_text.startswith(partial))):
        new_text = f' {new_text}'

    text_edit = lsp.TextEdit(
        range=lsp.Range(
            start=start,
            end=lsp.Position(line=position.line, character=start.character + len(partial or '')),
        ),
        new_text=new_text,
    )
    return [
        lsp.CompletionItem(
            label='scenario reference',
            kind=lsp.CompletionItemKind.Reference,
            deprecated=False,
            text_edit=text_edit,
            insert_text_format=lsp.InsertTextFormat.Snippet,
        )
    ]


def complete_variable_name(
    ls: GrizzlyLanguageServer,
    line: str,
    text_document: TextDocument,
    position: lsp.Position,
    *,
    partial: str | None = None,
) -> list[lsp.CompletionItem]:
    items: list[lsp.CompletionItem] = []

    ls.logger.debug(f'{line=}, {position=}, {partial=}')

    # find `Scenario:` before current position
    lines = text_document.source.splitlines()
    before_lines = reversed(lines[0 : position.line])

    for before_line in before_lines:
        if len(before_line.strip()) < 1:
            continue

        match = ls.variable_pattern.match(before_line)

        if match:
            variable_name = match.group(2) or match.group(3)

            if variable_name is None or (partial is not None and not variable_name.startswith(partial)):
                continue

            prefix = '' if partial is not None else '' if line[: position.character].endswith(' ') else ' '

            suffix = '"' if not line.rstrip().endswith('"') and line.count('"') % 2 != 0 else ''
            affix = '' if line[position.character :].strip().startswith('}}') else '}}'
            affix_suffix = '' if not line[position.character :].startswith('}}') and affix != '}}' else ' '
            new_text = f'{prefix}{variable_name}{affix_suffix}{affix}{suffix}'

            start = lsp.Position(
                line=position.line,
                character=position.character - len(partial or ''),
            )
            text_edit = lsp.TextEdit(
                range=lsp.Range(
                    start=start,
                    end=lsp.Position(
                        line=position.line,
                        character=start.character + len(partial or ''),
                    ),
                ),
                new_text=new_text,
            )

            logger.debug(f'{line=}, {variable_name=}, {partial=}, {text_edit=}')

            items.append(
                lsp.CompletionItem(
                    label=variable_name,
                    kind=lsp.CompletionItemKind.Variable,
                    deprecated=False,
                    text_edit=text_edit,
                )
            )
        elif any(scenario_keyword in before_line for scenario_keyword in ls.localizations.get('scenario', [])):
            break

    return items


def complete_step(
    ls: GrizzlyLanguageServer,
    keyword: str,
    position: lsp.Position,
    expression: str | None,
    *,
    base_keyword: str,
) -> list[lsp.CompletionItem]:
    # only suggest step expression related to the specific base keyword
    key = ls.get_language_key(base_keyword)
    steps = [step.expression for step in ls.steps.get(key, []) + ls.steps.get('step', [])]

    matched_steps: list[lsp.CompletionItem] = []
    matched_steps_1: set[str]
    matched_steps_2: set[str] = set()
    matched_steps_3: set[str] = set()

    if expression is None or len(expression) < 1:
        matched_steps_1 = set(steps)
    else:
        # remove any user values enclosed with double-quotes
        expression_shell = re.sub(r'"[^"]*"', '""', expression)

        # 1. exact matching
        matched_steps_1 = set(filter(lambda s: s.startswith(expression_shell), steps))

        if len(matched_steps_1) < 1 or ' ' not in expression:
            # 2. close enough matching
            matched_steps_2 = set(filter(lambda s: expression_shell in s, steps))

            # 3. "fuzzy" matching
            matched_steps_3 = set(get_close_matches(expression_shell, steps, len(steps), 0.6))

    # keep order so that 1. matches comes before 2. matches etc.
    matched_steps_container: dict[str, lsp.CompletionItem] = {}

    input_matches = list(re.finditer(r'"([^"]*)"', expression or '', flags=re.MULTILINE))

    for matched_step in itertools.chain(matched_steps_1, matched_steps_2, matched_steps_3):
        output_matches = list(re.finditer(r'"([^"]*)"', matched_step, flags=re.MULTILINE))

        # suggest step with already entetered variables in their correct place
        if input_matches and output_matches:
            offset = 0
            for input_match, output_match in zip(input_matches, output_matches, strict=False):
                matched_step = f'{matched_step[0 : output_match.start() + offset]}"{input_match.group(1)}"{matched_step[output_match.end() + offset :]}'  # noqa: PLW2901
                offset += len(input_match.group(1))

        start = lsp.Position(line=position.line, character=position.character)
        preselect: bool = False

        new_text = matched_step
        # completion triggered right next to keyword, no space
        # add space so keyword isn't overwritten
        if expression is None:
            new_text = f' {new_text}'

        # if matched step doesn't start what the user already had typed or we haven't removed
        # expression from matched step, we need to replace what already had been typed
        if (expression is not None and not new_text.startswith(expression)) or new_text == matched_step:
            character = start.character - len(str(expression))
            character = max(character, 0)
            start.character = character

        # do not suggest the step that is already written
        if matched_step == expression:
            continue
        if matched_step == new_text:  # exact match, preselect it
            preselect = True

        # if typed expression ends with whitespace, do not insert text starting with a whitespace
        if expression is not None and len(expression.strip()) > 0 and expression[-1] == ' ' and expression[-2] != ' ' and new_text[0] == ' ':
            new_text = new_text[1:]

        logger.debug(f'{expression=}, {new_text=}, {matched_step=}')

        if '""' in new_text:
            snippet_matches = re.finditer(
                r'""',
                new_text,
                flags=re.MULTILINE,
            )

            offset = 0
            for index, snippet_match in enumerate(snippet_matches, start=1):
                snippet_placeholder = f'${index}'
                new_text = f'{new_text[0 : snippet_match.start() + offset]}"{snippet_placeholder}"{new_text[snippet_match.end() + offset :]}'
                offset += len(snippet_placeholder)

            insert_text_format = lsp.InsertTextFormat.Snippet
        else:
            insert_text_format = lsp.InsertTextFormat.PlainText

        text_edit = lsp.TextEdit(
            range=lsp.Range(start=start, end=position),
            new_text=new_text,
        )

        matched_steps_container.update(
            {
                matched_step: lsp.CompletionItem(
                    label=matched_step,
                    kind=lsp.CompletionItemKind.Function,
                    documentation=ls._find_help(f'{keyword} {matched_step}'),
                    deprecated=False,
                    preselect=preselect,
                    insert_text_format=insert_text_format,
                    text_edit=text_edit,
                )
            }
        )

        matched_steps = list(matched_steps_container.values())

    return matched_steps
