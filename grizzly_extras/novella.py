import logging

from typing import Any, Dict, List, Union
from pathlib import Path

from novella.markdown.preprocessor import MarkdownPreprocessor, MarkdownFiles
from pydoc_markdown.novella.preprocessor import PydocTagPreprocessor
from pydoc_markdown.contrib.renderers.markdown import MarkdownRenderer

logger = logging.getLogger('grizzly.novella')


def make_human_readable(input: str) -> str:
    words: List[str] = []

    for word in input.split('_'):
        words.append(word.capitalize())

    output = ' '.join(words)

    for word in ['http', 'sftp', 'api', 'csv']:
        output = output.replace(word.capitalize(), word.upper())
        output = output.replace(word, word.upper())

    to_replace = dict(Iot='IoT', hub='Hub')
    for value, replace_value in to_replace.items():
        output = output.replace(value, replace_value)

    return output


def _create_nav_node(target: List[Union[str, Dict[str, str]]], path: str, node: Path) -> None:
    if not (node.is_file() and (node.stem == '__init__' or not node.stem.startswith('_'))):
        return

    if node.stem == '__init__':
        target.insert(0, f'{path}/index.md')
    else:
        target.append({make_human_readable(node.stem): f'{path}/{node.stem}.md'})


def mkdocs_update_config(config: Dict[str, Any]) -> None:
    root = Path.cwd().parent
    config_nav_tasks = config['nav'][3]['Framework'][0]['Usage'][0]['Tasks']
    config_nav_tasks_clients = config_nav_tasks.pop()
    tasks = root / 'grizzly' / 'tasks'

    nav_tasks: List[Union[str, Dict[str, str]]] = []
    for task in tasks.iterdir():
        _create_nav_node(nav_tasks, 'framework/usage/tasks', task)
    config_nav_tasks.extend(nav_tasks)

    tasks_clients = tasks / 'clients'
    nav_tasks_clients: List[Union[str, Dict[str, str]]] = []
    for task_client in tasks_clients.iterdir():
        _create_nav_node(nav_tasks_clients, 'framework/usage/tasks/clients', task_client)
    config_nav_tasks_clients['Clients'] = nav_tasks_clients
    config_nav_tasks.append(config_nav_tasks_clients)

    config_nav_testdata = config['nav'][3]['Framework'][0]['Usage'][1]['Variables'][1]
    nav_testdata: List[Union[str, Dict[str, str]]] = []
    variables = Path.cwd() / '..' / 'grizzly' / 'testdata' / 'variables'
    for variable in variables.iterdir():
        _create_nav_node(nav_testdata, 'framework/usage/variables/testdata', variable)
    config_nav_testdata['Testdata'] = nav_testdata

    config_nav_users = config['nav'][3]['Framework'][0]['Usage'][2]
    users = root / 'grizzly' / 'users'
    nav_users: List[Union[str, Dict[str, str]]] = []
    for user in users.iterdir():
        _create_nav_node(nav_users, 'framework/usage/load-users', user)

    config_nav_users['Load Users'] = nav_users

    config_nav_steps = config['nav'][3]['Framework'][0]['Usage'][3]['Steps']
    steps = root / 'grizzly' / 'steps'
    for step in steps.iterdir():
        _create_nav_node(config_nav_steps, 'framework/usage/steps', step)

    config_nav_steps_background = config_nav_steps[1]
    steps_background = steps / 'background'
    nav_steps_background: List[Union[str, Dict[str, str]]] = []
    for step in steps_background.iterdir():
        _create_nav_node(nav_steps_background, 'framework/usage/steps/background', step)

    config_nav_steps_background['Background'] = nav_steps_background

    config_nav_steps_scenario = config_nav_steps[2]
    steps_scenario = steps / 'scenario'
    nav_steps_scenario: List[Union[str, Dict[str, str]]] = []
    for step in steps_scenario.iterdir():
        _create_nav_node(nav_steps_scenario, 'framework/usage/steps/scenario', step)

    config_nav_steps_scenario['Scenario'] = nav_steps_scenario


def preprocess_markdown_update_with_header_levels(processor: MarkdownPreprocessor, levels: Dict[str, int]) -> None:
    if isinstance(processor, PydocTagPreprocessor) and isinstance(processor._renderer, MarkdownRenderer):
        processor._renderer.header_level_by_type.update(levels)


def _generate_dynamic_page(input_file: Path, output_path: Path, title: str, namespace: str) -> None:
    if not (input_file.is_file() and (input_file.stem == '__init__' or not input_file.stem.startswith('_'))):
        return

    if input_file.stem == '__init__':
        filename = 'index'
    else:
        filename = input_file.stem
        title = f'{title} / {make_human_readable(input_file.stem)}'
        namespace = f'{namespace}.{input_file.stem}'

    file = output_path / f'{filename}.md'
    file.parent.mkdir(parents=True, exist_ok=True)
    if not file.exists():
        file.write_text(f'''---
title: {title}
---
@pydoc {namespace}
''')


def generate_dynamic_pages(directory: Path) -> None:
    root = Path.cwd().parent

    tasks = root / 'grizzly' / 'tasks'
    output_path = directory / 'content' / 'framework' / 'usage' / 'tasks'
    for task in tasks.iterdir():
        _generate_dynamic_page(task, output_path, 'Tasks', 'grizzly.tasks')

    tasks_clients = tasks / 'clients'
    output_path = directory / 'content' / 'framework' / 'usage' / 'tasks' / 'clients'
    for task_client in tasks_clients.iterdir():
        _generate_dynamic_page(task_client, output_path, 'Clients', 'grizzly.tasks.clients')

    variables = root / 'grizzly' / 'testdata' / 'variables'
    output_path = directory / 'content' / 'framework' / 'usage' / 'variables' / 'testdata'
    for variable in variables.iterdir():
        _generate_dynamic_page(variable, output_path, 'Testdata', 'grizzly.testdata.variables')

    steps = root / 'grizzly' / 'steps'
    output_path = directory / 'content' / 'framework' / 'usage' / 'steps'
    for step in steps.iterdir():
        _generate_dynamic_page(step, output_path, 'Steps', 'grizzly.steps')

    steps_background = steps / 'background'
    output_path = directory / 'content' / 'framework' / 'usage' / 'steps' / 'background'
    for step in steps_background.iterdir():
        _generate_dynamic_page(step, output_path, 'Steps / Background', 'grizzly.steps.background')

    steps_scenario = steps / 'scenario'
    output_path = directory / 'content' / 'framework' / 'usage' / 'steps' / 'scenario'
    for step in steps_scenario.iterdir():
        _generate_dynamic_page(step, output_path, 'Steps / Scenario', 'grizzly.steps.scenario')

    users = root / 'grizzly' / 'users'
    output_path = directory / 'content' / 'framework' / 'usage' / 'load-users'
    for user in users.iterdir():
        _generate_dynamic_page(user, output_path, 'Load Users', 'grizzly.users')


class GrizzlyMarkdownProcessor(MarkdownPreprocessor):
    def process_files(self, files: MarkdownFiles) -> None:
        for file in files:
            logger.info(f'!! {file.path=}, {file.output_path=}')
