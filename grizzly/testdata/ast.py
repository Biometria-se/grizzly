"""Contains methods for handling AST operations when parsing templates."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from jinja2 import nodes as j2

from grizzly.context import GrizzlyContextScenario

from . import GrizzlyVariables

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator, Iterable

    from grizzly.context import GrizzlyContext

logger = logging.getLogger(__name__)


class AstVariableNameSet(set[str]):
    def add(self, variable: str) -> None:
        if '.' in variable:
            variable = GrizzlyVariables.get_initialization_value(variable)

            super().add(variable)
        else:
            super().add(variable)

    def update(self, *args: Iterable[str]) -> None:
        for arg in args:
            for a in arg:
                self.add(a)


class AstVariableSet(dict[GrizzlyContextScenario, set[str]]):
    __conditional__: AstVariableNameSet
    __map__: dict[str, str]
    __init_map__: dict[str, set[str]]
    __local__: set[str]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.__conditional__ = AstVariableNameSet()
        self.__local__ = AstVariableNameSet()
        self.__map__ = {}
        self.__init_map__ = {}

    def register(self, scenario: GrizzlyContextScenario, variable: str) -> None:
        initialization_value = GrizzlyVariables.get_initialization_value(variable)

        # map variable name with the value it was initialized with
        self.__map__.update({variable: initialization_value})
        if initialization_value not in self.__init_map__:
            self.__init_map__.update({initialization_value: set()})

        # initialized value to variable name -- initialization values for objects
        self.__init_map__[initialization_value].add(variable)

        if scenario not in self:
            self.update({scenario: set()})

        self[scenario].add(variable)


def get_template_variables(grizzly: GrizzlyContext) -> dict[GrizzlyContextScenario, set[str]]:
    """Get all templates per scenario and parse them to find all variables that are used.

    Variables can be found in templates, but be used in context which allows then to not necessary not
    being defined. Also, the testdata producer only serves testdata for variables that has been
    declared/initialized.

    Example:
    ```csv title="input.csv"
    name,quirk
    alice,late
    bob,tired
    charlie,bossy
    ```

    ```gherkin
    Given value of variable "AtomicCsvReader.input" is "input.csv"
    And value of variable "foobar" is "True"
    And value for variable "AtomicIntegerIncrementer.id" is "1"
    ```

    The declared variables are:
    - `AtomicIntegerIncrementer.id`
    - `AtomicCsvReader.input`
    - `foobar`

    ```plain
    {% set quirk = AtomicCsvReader.input.quirk if AtomicCsvReader.input is defined else "none" %}
    {% set name = AtomicCsvReader.input.name if AtomicCsvReader.input is defined else "none" %}
    {
        "id": {{ AtomicIntegerIncrementer.id }},
        "name": "{{ name }}",
        "quirk": "{{ quirk }}",
        "foobar": {{ foobar }}
    }
    ```

    The variables used in the template are:
    - `AtomicCsvReader.input`
    - `AtomicCsvReader.input.quirk`
    - `AtomicCsvReader.input.name`
    - `AtomicIntegerIncrementer.id`
    - `name`
    - `quirk`
    - `foobar`

    Variables served by the `TestdataProducer` should be:
    - `AtomicCsvReader.input.quirk`
    - `AtomicCsvReader.input.name`
    - `AtomicIntegerIncrementer.id`
    - `foobar`

    We must then align the declared and found variables, so that `TestdataProducer` only gets variables that actually has been declared.

    Also there are support for "internal" variables, which can be implementation specific that we should ignore here. These variables have dunder names, e.g.
    a prefix and suffix being double underscore (`__`), and example is `__message__.id`, these templates will be left as-is, and needs to be rendered just in
    time before they are used.

    """
    templates: dict[GrizzlyContextScenario, set[str]] = {}

    for _scenario in grizzly.scenarios:
        if _scenario not in templates:
            templates[_scenario] = set()

        for task in _scenario.tasks():
            templates[_scenario].update(task.get_templates())

        templates[_scenario].update(_scenario.orphan_templates)

        if len(templates[_scenario]) == 0:
            del templates[_scenario]

    return parse_templates(templates)


def parse_templates(templates: dict[GrizzlyContextScenario, set[str]], *, check_declared: bool = True) -> dict[GrizzlyContextScenario, set[str]]:
    template_variables = _parse_templates(templates)

    for scenario, variables in template_variables.items():
        found_variables = AstVariableNameSet()
        found_variables.update(variables)

        declared_variables = AstVariableNameSet()
        declared_variables.update(scenario.variables)

        if check_declared:
            # check except between declared variables and variables found in templates
            missing_in_templates = {variable for variable in declared_variables if variable not in found_variables} - template_variables.__conditional__
            missing_in_templates_message = '\n'.join(sorted(missing_in_templates))
            assert len(missing_in_templates) == 0, f'variables have been declared, but cannot be found in templates:\n{missing_in_templates_message}'

        # check if any variable hasn't first been declared
        missing_declarations = {variable for variable in found_variables if variable not in declared_variables} - template_variables.__conditional__ - template_variables.__local__
        missing_declarations_message = '\n'.join(sorted(missing_declarations))
        assert len(missing_declarations) == 0, f'variables have been found in templates, but have not been declared:\n{missing_declarations_message}'

    # only include variables that has been declared, filtering out conditional ones
    filtered_template_variables: dict[GrizzlyContextScenario, set[str]] = {}

    for scenario, scenario_variables in template_variables.items():
        filtered_template_variables.update({scenario: set()})

        for scenario_variable in scenario_variables:
            variable_check = template_variables.__map__.get(scenario_variable, '__None__')

            # variable must have been declared
            if variable_check not in scenario.variables:
                continue

            initilization_values = template_variables.__init_map__.get(scenario_variable, set())

            # if this variable is an object, has has sub-variables
            if len(initilization_values) > 1 or (len(initilization_values) == 1 and {variable_check} != initilization_values):
                continue

            filtered_template_variables[scenario].add(scenario_variable)

    return filtered_template_variables


def walk_attr(node: j2.Getattr) -> list[str]:
    """Recursivley walk an AST node to get a complete variable name."""

    def _walk_attr(parent: j2.Getattr) -> list[str]:
        attributes: list[str] = []
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


def _parse_templates(templates: dict[GrizzlyContextScenario, set[str]]) -> AstVariableSet:  # noqa: C901, PLR0915
    variables = AstVariableSet()

    def _getattr(node: j2.Node) -> Generator[list[str], None, None]:  # noqa: C901, PLR0912, PLR0915
        attributes: list[str] | None = None

        if isinstance(node, j2.Getattr):
            child_node = getattr(node, 'node', None)
            if child_node is not None:
                if isinstance(child_node, j2.Getattr | j2.Name):
                    attributes = walk_attr(node)
                else:
                    yield from _getattr(child_node)
        elif isinstance(node, j2.Getitem):
            child_node = getattr(node, 'node', None)
            child_node_name = getattr(child_node, 'name', None)

            if child_node is not None:
                yield from _getattr(child_node)
            elif child_node_name is not None:
                attributes = [child_node_name]
        elif isinstance(node, j2.Assign):  #  {% set variable = value %} expressions
            target_node = getattr(node, 'target', None)
            if target_node is not None:
                attrs_generator = list(_getattr(target_node))
                for attrs in attrs_generator:
                    internal_variable = _build_variable(attrs)
                    if internal_variable is not None:
                        variables.__local__.add(internal_variable)

            child_node = getattr(node, 'node', None)
            if child_node is not None:
                yield from _getattr(child_node)
        elif isinstance(node, j2.Call):
            args = getattr(node, 'args', [])
            for arg in args:
                yield from _getattr(arg)

            child_node = getattr(node, 'node', None)
            if child_node is not None and not isinstance(child_node, j2.Name):
                yield from _getattr(child_node)
        elif isinstance(node, j2.Name):
            name = getattr(node, 'name', None)
            if name is not None:
                attributes = [name]
        elif isinstance(node, j2.Filter | j2.UnaryExpr):
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

            if isinstance(test_node, j2.Not):
                test_node = getattr(test_node, 'node', test_node)

            if test_node is not None:
                yield from _getattr(test_node)

            could_be_undefined = getattr(test_node, 'name', '') == 'defined'

            expr_node = getattr(node, 'expr1', None)
            if expr_node is not None:
                attrs_generator = list(_getattr(expr_node))
                for attrs in attrs_generator if could_be_undefined else []:
                    undefined_variable = _build_variable(attrs)
                    if undefined_variable is not None:
                        variables.__conditional__.add(undefined_variable)

                yield from attrs_generator

            expr_node = getattr(node, 'expr2', None)
            if expr_node is not None:
                attrs_generator = list(_getattr(expr_node))
                for attrs in attrs_generator if could_be_undefined else []:
                    undefined_variable = _build_variable(attrs)
                    if undefined_variable is not None:
                        variables.__conditional__.add(undefined_variable)

                yield from attrs_generator
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

            for n in nodes:
                yield from _getattr(n)
        elif isinstance(node, j2.Test):
            name = getattr(node, 'name', None)
            child_node = getattr(node, 'node', None)
            if child_node is not None:
                yield from _getattr(child_node)

            for arg in getattr(node, 'args', []):
                yield from _getattr(arg)
        elif isinstance(node, j2.List):
            for item in getattr(node, 'items', []):
                yield from _getattr(item)
        elif isinstance(node, j2.Call):
            child_node = getattr(node, 'node', None)
            if child_node is not None:
                yield from _getattr(child_node)
        elif isinstance(node, j2.If):
            child_node = getattr(node, 'test', None)
            if child_node is not None:
                yield from _getattr(child_node)
        elif isinstance(node, j2.Output):
            for child_node in getattr(node, 'nodes', []):
                yield from _getattr(child_node)
        elif not isinstance(node, j2.Const | j2.TemplateData | j2.For):  # all unhandled AST nodes
            logger.warning('unhandled AST node: %r', node)

        if attributes is not None:
            yield attributes

    def _build_variable(attributes: list[str]) -> str | None:
        if len(attributes) == 0:
            return None

        # ignore builtin modules
        if attributes[0] in ['datetime']:
            return None

        # ignore builtin methods calls
        if attributes[-1] in ['replace']:
            attributes = attributes[:-1]

        return '.'.join(attributes)

    for scenario, template_sources in templates.items():
        if scenario not in variables:
            variables[scenario] = set()

        # can raise TemplateError which should be handled else where
        for template in template_sources:
            # json.dumps escapes quote (") causing it to be \\", which inturn causes problems for jinja
            template_normalized = template.replace('\\"', "'")

            parsed = scenario.jinja2.parse(template_normalized)

            for body in getattr(parsed, 'body', []):
                for attributes in _getattr(body):
                    variable = _build_variable(attributes)
                    if variable is None or (  # check if variable is has a dunder name, and ignore it (internal variable)
                        len(variable) > 4 and variable.startswith('__') and variable.index('__') != variable.rindex('__')
                    ):
                        continue

                    variables.register(scenario, variable)

    return variables
