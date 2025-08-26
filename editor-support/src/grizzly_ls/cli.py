from __future__ import annotations

import sys
import traceback
from pathlib import Path
from typing import TYPE_CHECKING

from colorama import Fore
from colorama import init as colorama_init
from lsprotocol.types import Diagnostic, DiagnosticSeverity
from pygls.workspace import TextDocument

from grizzly_ls.server.commands import render_gherkin
from grizzly_ls.server.features.diagnostics import validate_gherkin
from grizzly_ls.server.inventory import compile_inventory
from grizzly_ls.text import find_language

if TYPE_CHECKING:  # pragma: no cover
    from argparse import Namespace as Arguments

    from grizzly_ls.server import GrizzlyLanguageServer


def _get_severity_color(severity: DiagnosticSeverity | None) -> str:
    if severity == DiagnosticSeverity.Error:
        return Fore.RED
    if severity == DiagnosticSeverity.Information:
        return Fore.BLUE
    if severity == DiagnosticSeverity.Warning:
        return Fore.YELLOW
    if severity == DiagnosticSeverity.Hint:
        return Fore.CYAN

    return Fore.RESET


def diagnostic_to_text(filename: str, diagnostic: Diagnostic, max_length: int) -> str:
    color = _get_severity_color(diagnostic.severity)
    severity = diagnostic.severity.name if diagnostic.severity is not None else 'unknown'
    message = ': '.join(diagnostic.message.split('\n'))

    message_file = f'{filename}:{diagnostic.range.start.line + 1}:{diagnostic.range.start.character + 1}'
    message_severity = f'{color}{severity.lower()}{Fore.RESET}'

    # take line number into consideration, max 9999:9999
    max_length += 9

    # color and reset codes makes the string 10 bytes longer than the actual text length -+
    #                                                                                     |
    #                                                        v----------------------------+
    return f'{message_file:<{max_length}} {message_severity:<17} {message}'


def lint(ls: GrizzlyLanguageServer, args: Arguments) -> int:
    files: list[Path]

    # init colorama for ansi colors
    colorama_init()

    # init language server
    ls.root_path = Path.cwd()
    ls.logger.logger.handlers = []
    ls.logger.logger.propagate = False
    compile_inventory(ls, standalone=True)

    if args.files == ['.']:
        files = list(ls.root_path.rglob('**/*.feature'))
    else:
        files = []
        paths = [Path(file) for file in args.files]

        for path in paths:
            file = Path(path)

            if file.is_dir():
                files.extend(list(file.rglob('**/*.feature')))
            else:
                files.append(file)

    rc: int = 0
    grouped_diagnostics: dict[str, list[Diagnostic]] = {}
    max_length = 0

    for file in sorted(files):
        text_document = TextDocument(file.resolve().as_uri())
        try:
            ls.language = find_language(text_document.source)
        except ValueError:  # pragma: no cover
            ls.language = 'en'

        diagnostics = validate_gherkin(ls, text_document)

        if len(diagnostics) < 1:
            continue

        text_document_file = Path(text_document.uri.removeprefix('file://'))
        filename = text_document_file.as_posix().replace(Path.cwd().as_posix(), '').lstrip('/\\')
        max_length = max(max_length, len(filename))

        grouped_diagnostics.update({filename: diagnostics})

    if len(grouped_diagnostics) > 0:
        rc = 1

    for filename, diagnostics in grouped_diagnostics.items():
        print('\n'.join(diagnostic_to_text(filename, diagnostic, max_length) for diagnostic in diagnostics))

    return rc


def render(args: Arguments) -> int:
    try:
        feature_file = Path(args.file[0])

        if not feature_file.exists():
            print(f'{feature_file.as_posix()} does not exist', file=sys.stderr)
            return 1
    except IndexError:
        print('no file specified', file=sys.stderr)
        return 1

    try:
        print(render_gherkin(feature_file.as_posix(), feature_file.read_text(), raw=True))
    except:
        print(traceback.format_exc(), file=sys.stderr)
        return 1
    else:
        return 0
