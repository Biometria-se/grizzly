import logging

from typing import Set, Optional, List, Dict

import jinja2 as j2

from jinja2.nodes import Getattr, Getitem, Name

from ..tasks import GrizzlyTask
from ..context import GrizzlyContextScenario

logger = logging.getLogger(__name__)


def get_template_variables(tasks: List[GrizzlyTask]) -> Dict[str, Set[str]]:
    templates: Dict[GrizzlyContextScenario, Set[str]] = {}

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


def _parse_templates(templates: Dict[GrizzlyContextScenario, Set[str]]) -> Dict[str, Set[str]]:
    variables: Dict[str, Set[str]] = {}

    for scenario, scenario_templates in templates.items():
        scenario_name = scenario.get_name()

        if scenario_name not in variables:
            variables[scenario_name] = set()

        has_processed_orphan_templates = False

        # can raise TemplateError which should be handled else where
        for template in scenario_templates:
            j2env = j2.Environment(
                autoescape=False,
                loader=j2.FileSystemLoader('.'),
            )

            sources: List[str] = [template]

            if not has_processed_orphan_templates:
                sources += scenario.orphan_templates
                has_processed_orphan_templates = True

            for source in sources:
                parsed = j2env.parse(source)

                for body in getattr(parsed, 'body', []):
                    for node in getattr(body, 'nodes', []):
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

                        if attributes is not None:
                            variables[scenario_name].add('.'.join(attributes))

    return variables
