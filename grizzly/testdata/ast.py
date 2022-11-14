import logging

from typing import TYPE_CHECKING, Set, Optional, List, Dict, Generator

import jinja2 as j2

from jinja2.nodes import Getattr, Getitem, Name, Compare, Filter, Node, Mod, Add, Mul, Sub

from ..tasks import GrizzlyTask


if TYPE_CHECKING:
    from ..context import GrizzlyContextScenario

logger = logging.getLogger(__name__)


def get_template_variables(tasks: List[GrizzlyTask]) -> Dict[str, Set[str]]:
    templates: Dict['GrizzlyContextScenario', Set[str]] = {}

    for task in tasks:
        if task.scenario not in templates:
            templates[task.scenario] = set()

        for template in task.get_templates():
            templates[task.scenario].add(template)

    return _parse_templates(templates)


def walk_attr(node: Getattr) -> List[str]:
    def _walk_attr(parent: Getattr) -> List[str]:
        attributes: List[str] = [getattr(parent, 'attr')]
        child = getattr(parent, 'node')

        if isinstance(child, Getattr):
            attributes += _walk_attr(child)
        elif isinstance(child, Name):
            attributes.append(getattr(child, 'name'))

        return attributes

    attributes = _walk_attr(node)
    attributes.reverse()

    return attributes


def _parse_templates(templates: Dict['GrizzlyContextScenario', Set[str]]) -> Dict[str, Set[str]]:
    variables: Dict[str, Set[str]] = {}

    for scenario, scenario_templates in templates.items():
        scenario_name = scenario.class_name

        if scenario_name not in variables:
            variables[scenario_name] = set()

        def _getattr(node: Node) -> Generator[List[str], None, None]:
            attributes: Optional[List[str]] = None

            if isinstance(node, Getattr):
                attributes = walk_attr(node)
            elif isinstance(node, Getitem):
                child_node = getattr(node, 'node')
                child_node_name = getattr(child_node, 'name', None)
                if child_node_name is not None:
                    attributes = [child_node_name]
            elif isinstance(node, Name):
                attributes = [getattr(node, 'name')]
            elif isinstance(node, Filter):
                child_node = getattr(node, 'node')
                yield from _getattr(child_node)
            elif isinstance(node, (Mod, Add, Mul, Sub)):
                left_node = getattr(node, 'left')
                yield from _getattr(left_node)
                right_node = getattr(node, 'right')
                yield from _getattr(right_node)
            elif isinstance(node, Compare):
                expr = getattr(node, 'expr')
                if isinstance(expr, Filter):
                    node = getattr(expr, 'node')
                    yield from _getattr(node)
                else:
                    raise ValueError(f'cannot find variable name in {parsed}')

            if attributes is not None:
                yield attributes

        template_sources = list(scenario_templates) + scenario.orphan_templates

        # can raise TemplateError which should be handled else where
        for template in template_sources:
            j2env = j2.Environment(
                autoescape=False,
                loader=j2.FileSystemLoader('.'),
            )

            parsed = j2env.parse(template)

            for body in getattr(parsed, 'body', []):
                for node in getattr(body, 'nodes', []):
                    for attributes in _getattr(node):
                        variables[scenario_name].add('.'.join(attributes))

    return variables
