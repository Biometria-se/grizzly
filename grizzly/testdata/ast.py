import logging

from typing import Set, Optional, List, Dict, Tuple, Union, cast

import jinja2 as j2

from jinja2.nodes import Getattr, Getitem, Name

from ..task import RequestTask


RequestSourceMapping = Dict[str, Set[Tuple[str, Union[str, RequestTask]]]]

logger = logging.getLogger(__name__)


def _get_template_variables_from_request_task(requests: List[RequestTask]) -> Dict[str, Set[Tuple[str, RequestTask]]]:
    templates: Dict[str, Set[Tuple[str, RequestTask]]] = {}

    for request in requests:
        scenario = request.scenario.get_name()

        if scenario not in templates:
            templates[scenario] = set()

        templates[scenario].add(('.', request))

    return templates


def get_template_variables(sources: Optional[List[RequestTask]]) -> Dict[str, Set[str]]:
    templates: RequestSourceMapping

    if sources is None or len(sources) == 0:
        templates = {}
    else:
        templates = cast(
            RequestSourceMapping,
            _get_template_variables_from_request_task(sources),
        )

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


def _parse_templates(requests: RequestSourceMapping) -> Dict[str, Set[str]]:
    variables: Dict[str, Set[str]] = {}

    for scenario, scenario_requests in requests.items():
        if scenario not in variables:
            variables[scenario] = set()

        has_processed_orphan_templates = False

        # can raise TemplateError which should be handled else where
        for (path, request) in scenario_requests:
            j2env = j2.Environment(
                autoescape=False,
                loader=j2.FileSystemLoader(path),
            )

            sources: List[str] = []
            template_source: Optional[str] = None
            # get template source
            if isinstance(request, RequestTask):
                if request.source is not None:
                    template_source = request.source
                sources += [request.name, request.endpoint]
            else:
                template_source = cast(j2.BaseLoader, j2env.loader).get_source(j2env, request)[0]

            if template_source is not None:
                sources.append(template_source)

            if not has_processed_orphan_templates and isinstance(request, RequestTask):
                sources += request.scenario.orphan_templates
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
                            variables[scenario].add('.'.join(attributes))

    return variables
