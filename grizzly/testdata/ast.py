"""Contains methods for handling AST operations when parsing templates."""
from __future__ import annotations

import itertools
import logging
from typing import TYPE_CHECKING, Dict, Generator, List, Optional, Set, Tuple

from jinja2 import Environment as Jinja2Environment
from jinja2 import nodes as j2

from . import GrizzlyVariables

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext, GrizzlyContextScenario

logger = logging.getLogger(__name__)


def get_template_variables(grizzly: GrizzlyContext) -> dict[str, set[str]]:
    """Get all templates per scenario and parse them to find all variables that are used."""
    templates: dict[GrizzlyContextScenario, set[str]] = {}

    for _scenario in grizzly.scenarios:
        if _scenario not in templates:
            templates[_scenario] = set()

        for task in _scenario.tasks():
            templates[_scenario].update(task.get_templates())

        templates[_scenario].update(_scenario.orphan_templates)

        if len(templates[_scenario]) == 0:
            del templates[_scenario]

    template_variables, allowed_unused = _parse_templates(templates, env=grizzly.state.jinja2)

    found_variables = set()
    for variable in itertools.chain(*template_variables.values()):
        module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(variable)

        if module_name is None and variable_type is None:
            found_variables.add(variable_name)
        else:
            prefix = f'{module_name}.' if module_name != 'grizzly.testdata.variables' else ''
            found_variables.add(f'{prefix}{variable_type}.{variable_name}')

    declared_variables = set(grizzly.state.variables.keys())

    # check except between declared variables and variables found in templates
    missing_in_templates = {variable for variable in declared_variables if variable not in found_variables} - allowed_unused
    assert len(missing_in_templates) == 0, f'variables has been declared, but cannot be found in templates: {",".join(missing_in_templates)}'

    missing_declarations = [variable for variable in found_variables if variable not in declared_variables]
    assert len(missing_declarations) == 0, f'variables has been found in templates, but have not been declared: {",".join(missing_declarations)}'

    return template_variables


def walk_attr(node: j2.Getattr) -> List[str]:
    """Recursivley walk an AST node to get a complete variable name."""

    def _walk_attr(parent: j2.Getattr) -> List[str]:
        attributes: List[str] = []
        attr = getattr(parent, 'attr', None)
        if attr is not None:
            attributes.append(attr)

        child = getattr(parent, 'node', None)

        if isinstance(child, j2.Getattr):
            attributes += _walk_attr(child)
        elif isinstance(child, j2.Name):
            name = getattr(child, 'name', None)
            if name is not None:
                attributes.append(name)

        return attributes

    attributes = _walk_attr(node)
    attributes.reverse()

    return attributes


def _parse_templates(templates: Dict[GrizzlyContextScenario, Set[str]], *, env: Jinja2Environment) -> Tuple[Dict[str, Set[str]], Set[str]]:  # noqa: C901, PLR0915
    variables: Dict[str, Set[str]] = {}
    allowed_undefined: Set[str] = set()

    def _getattr(node: j2.Node) -> Generator[List[str], None, None]:  # noqa: C901, PLR0912, PLR0915
        attributes: Optional[List[str]] = None

        if isinstance(node, j2.Getattr):
            child_node = getattr(node, 'node', None)
            if child_node is not None:
                if isinstance(child_node, (j2.Getattr, j2.Name)):
                    attributes = walk_attr(node)
                else:
                    yield from _getattr(child_node)
        elif isinstance(node, j2.Getitem):
            child_node = getattr(node, 'node', None)
            child_node_name = getattr(child_node, 'name', None)
            if child_node_name is None and child_node is not None:
                yield from _getattr(child_node)
            elif child_node_name is not None:
                attributes = [child_node_name]
        elif isinstance(node, j2.Name):
            name = getattr(node, 'name', None)
            if name is not None:
                attributes = [name]
        elif isinstance(node, (j2.Filter, j2.UnaryExpr)):
            child_node = getattr(node, 'node', None)
            if child_node is not None:
                yield from _getattr(child_node)
        elif isinstance(node, j2.BinExpr):
            left_node = getattr(node, 'left', None)
            if left_node is not None:
                yield from _getattr(left_node)
            right_node = getattr(node, 'right', None)
            if right_node is not None:
                yield from _getattr(right_node)
        elif isinstance(node, j2.CondExpr):
            test_node = getattr(node, 'test', None)
            if test_node is not None:
                yield from _getattr(test_node)

            expr_node = getattr(node, 'expr1', None)
            if expr_node is not None:
                yield from _getattr(expr_node)

            expr_node = getattr(node, 'expr2', None)
            if expr_node is not None:
                yield from _getattr(expr_node)
        elif isinstance(node, j2.Compare):
            expr = getattr(node, 'expr', None)
            if expr is not None:
                yield from _getattr(expr)

            ops = getattr(node, 'ops', [])
            for op in ops:
                yield from _getattr(op)
        elif isinstance(node, j2.Operand):
            expr = getattr(node, 'expr', None)

            if expr is not None:
                yield from _getattr(expr)
        elif isinstance(node, j2.Concat):
            nodes = getattr(node, 'nodes', [])

            for node in nodes:
                yield from _getattr(node)
        elif isinstance(node, j2.Test):
            name = getattr(node, 'name', None)
            child_node = getattr(node, 'node', None)
            if child_node is not None:
                child_attributes = list(_getattr(child_node))
                # attributes of a variable was part of a "is defined"-test
                # keep track so we can ignore it later, which will allow
                # as long as there is a test for it undefined variables
                if name == 'defined':
                    allowed_undefined.add('.'.join(child_attributes[0]))

                yield from child_attributes

            for arg in getattr(node, 'args', []):
                yield from _getattr(arg)
        elif isinstance(node, j2.List):
            for item in getattr(node, 'items', []):
                yield from _getattr(item)
        elif isinstance(node, j2.Call):
            child_node = getattr(node, 'node', None)
            if child_node is not None:
                yield from _getattr(child_node)

        if attributes is not None:
            yield attributes

    for scenario, template_sources in templates.items():
        scenario_name = scenario.class_name

        if scenario_name not in variables:
            variables[scenario_name] = set()

        # can raise TemplateError which should be handled else where
        for template in template_sources:
            # json.dumps escapes quote (") causing it to be \\", which inturn causes problems for jinja
            template_normalized = template.replace('\\"', "'")

            parsed = env.parse(template_normalized)

            for body in getattr(parsed, 'body', []):
                if isinstance(body, j2.Assign):  #  {% .. %} expressions
                    node = getattr(body, 'node', None)
                    if node is None:
                        continue

                    for attributes in _getattr(node):
                        variables[scenario_name].add('.'.join(attributes))
                else:
                    for node in getattr(body, 'nodes', []):
                        for attributes in _getattr(node):
                            branches = attributes

                            # ignore builtin modules
                            if branches[0] in ['datetime']:
                                continue

                            # ignore builtin methods calls
                            if branches[-1] in ['replace']:
                                branches = branches[:-1]

                            variable = '.'.join(branches)
                            if variable in allowed_undefined:
                                continue

                            variables[scenario_name].add(variable)

    return variables, allowed_undefined
