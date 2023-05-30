import logging

from typing import TYPE_CHECKING, Set, Optional, List, Dict, Generator

from jinja2 import Environment as Jinja2Environment, FileSystemLoader as Jinja2FileSystemLoader
from jinja2 import nodes as j2


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContextScenario, GrizzlyContext

logger = logging.getLogger(__name__)


def get_template_variables(grizzly: 'GrizzlyContext') -> Dict[str, Set[str]]:
    templates: Dict['GrizzlyContextScenario', Set[str]] = {}

    for scenario in grizzly.scenarios:
        if scenario not in templates:
            templates[scenario] = set()

        for task in scenario.tasks():
            for template in task.get_templates():
                templates[scenario].add(template)

        templates[scenario].update(scenario.orphan_templates)

        if len(templates[scenario]) == 0:
            del templates[scenario]

    return _parse_templates(templates)


def walk_attr(node: j2.Getattr) -> List[str]:
    def _walk_attr(parent: j2.Getattr) -> List[str]:
        attributes: List[str] = [getattr(parent, 'attr')]
        child = getattr(parent, 'node')

        if isinstance(child, j2.Getattr):
            attributes += _walk_attr(child)
        elif isinstance(child, j2.Name):
            attributes.append(getattr(child, 'name'))

        return attributes

    attributes = _walk_attr(node)
    attributes.reverse()

    return attributes


def _parse_templates(templates: Dict['GrizzlyContextScenario', Set[str]]) -> Dict[str, Set[str]]:
    variables: Dict[str, Set[str]] = {}

    def _getattr(node: j2.Node) -> Generator[List[str], None, None]:
        attributes: Optional[List[str]] = None

        if isinstance(node, j2.Getattr):
            attributes = walk_attr(node)
        elif isinstance(node, j2.Getitem):
            child_node = getattr(node, 'node')
            child_node_name = getattr(child_node, 'name', None)
            if child_node_name is not None:
                attributes = [child_node_name]
        elif isinstance(node, j2.Name):
            attributes = [getattr(node, 'name')]
        elif isinstance(node, (j2.Filter, j2.UnaryExpr,)):
            child_node = getattr(node, 'node')
            yield from _getattr(child_node)
        elif isinstance(node, j2.BinExpr):
            left_node = getattr(node, 'left')
            yield from _getattr(left_node)
            right_node = getattr(node, 'right')
            yield from _getattr(right_node)
        elif isinstance(node, j2.CondExpr):
            test_node = getattr(node, 'test')
            yield from _getattr(test_node)

            expr_node = getattr(node, 'expr1')
            yield from _getattr(expr_node)

            expr_node = getattr(node, 'expr2')
            if expr_node is not None:
                yield from _getattr(expr_node)
        elif isinstance(node, j2.Compare):
            expr = getattr(node, 'expr')
            yield from _getattr(expr)

            ops = getattr(node, 'ops', [])
            for op in ops:
                yield from _getattr(op)
        elif isinstance(node, j2.Operand):
            expr = getattr(node, 'expr')

            yield from _getattr(expr)
        elif isinstance(node, j2.Concat):
            nodes = getattr(node, 'nodes')

            for node in nodes:
                yield from _getattr(node)
        elif isinstance(node, j2.Test):
            child_node = getattr(node, 'node')
            yield from _getattr(child_node)

            args = getattr(node, 'args')
            for arg in args:
                yield from _getattr(arg)
        elif isinstance(node, j2.List):
            for item in getattr(node, 'items', []):
                yield from _getattr(item)

        if attributes is not None:
            yield attributes

    for scenario, template_sources in templates.items():
        scenario_name = scenario.class_name

        if scenario_name not in variables:
            variables[scenario_name] = set()

        # can raise TemplateError which should be handled else where
        for template in template_sources:
            j2env = Jinja2Environment(
                autoescape=False,
                loader=Jinja2FileSystemLoader('.'),
            )

            # json.dumps escapes quote (") causing it to be \\", which inturn causes problems for jinja
            template = template.replace('\\"', "'")

            parsed = j2env.parse(template)

            for body in getattr(parsed, 'body', []):
                for node in getattr(body, 'nodes', []):
                    for attributes in _getattr(node):
                        variables[scenario_name].add('.'.join(attributes))

    return variables
