"""Functionality for loading grizzly configuration.
- from keyvault
- merging with other configuration files
- ...
"""

from __future__ import annotations

import re
from base64 import b64decode
from contextlib import suppress
from pathlib import Path
from shutil import which
from textwrap import dedent
from typing import TYPE_CHECKING, ClassVar, cast

import yaml
from azure.identity import AzureCliCredential, ChainedTokenCredential, ManagedIdentityCredential
from azure.keyvault.secrets import KeyVaultSecret, SecretClient
from behave.parser import parse_feature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives._serialization import PBES, KeySerializationEncryption, KeySerializationEncryptionBuilder, PrivateFormat
from cryptography.hazmat.primitives.serialization import pkcs12
from jinja2 import Environment
from jinja2.lexer import Token, TokenStream
from jinja2_simple_tags import StandaloneTag

from grizzly_cli.utils import IndentDumper, logger, merge_dicts, run_command, unflatten

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from behave.model import Scenario
    from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
    from cryptography.x509 import Certificate


def get_context_root() -> Path:
    possible_context_roots = Path.cwd().rglob('environment.py')

    context_root: Path | None = None

    for possible_context_root in possible_context_roots:
        if any(ignore in possible_context_root.as_posix() for ignore in ['.venv', '.env', 'node_modules']):
            continue

        if context_root is None:
            context_root = possible_context_root
            continue

        if possible_context_root.as_posix().count('/') < context_root.as_posix().count('/'):
            context_root = possible_context_root

    if context_root is None:
        message = 'context root not found, are you in a grizzly project?'
        raise ValueError(message)

    return context_root.parent


class ScenarioTag(StandaloneTag):
    tags: ClassVar[set[str]] = {'scenario'}

    def preprocess(
        self,
        source: str,
        name: str | None,
        filename: str | None = None,
    ) -> str:
        self._source = source

        return cast('str', super().preprocess(source, name, filename))

    @classmethod
    def get_scenario_text(cls, name: str, file: Path) -> str:
        content = file.read_text()

        content_skel = re.sub(r'\{%.*%\}', '', content)
        content_skel = re.sub(r'\{\$.*\$\}', '', content_skel)

        assert len(content.splitlines()) == len(content_skel.splitlines()), 'oops, there is not a 1:1 match between lines!'

        feature = parse_feature(content_skel, filename=file.as_posix())
        scenarios = cast('list[Scenario]', feature.scenarios)
        lines = content.splitlines()

        scenario_index: int
        for index, scenario in enumerate(scenarios):
            if scenario.name == name:
                scenario_index = index
                break

        # check if there are scenarios after our scenario in the source
        next_scenario: Scenario | None = None
        with suppress(IndexError):
            next_scenario = scenarios[scenario_index + 1]

        if next_scenario is None:  # last scenario, take everything until the end
            scenario_lines = lines[scenario.line :]
        else:  # take everything up until where the next scenario starts
            scenario_lines = lines[scenario.line : next_scenario.line - 1]
            if scenario_lines[-1] == '':  # if last line is an empty line, lets remove it
                scenario_lines.pop()

        # remove any scenario text/comments
        if scenario_lines[0].strip() == '"""':
            try:
                offset = scenario_lines[1:].index(scenario_lines[0]) + 1 + 1
            except:
                offset = 0

            scenario_lines = scenario_lines[offset:]

        # first line can have incorrect indentation
        scenario_lines[0] = dedent(scenario_lines[0])

        return '\n'.join(scenario_lines)

    def render(self, scenario: str, feature: str, **variables: str) -> str:
        feature_file = Path(feature)

        # check if relative to parent feature file
        if not feature_file.exists():
            feature_file = (self.environment.feature_file.parent / feature).resolve()

        scenario_content = self.get_scenario_text(scenario, feature_file)

        ignore_errors = getattr(self.environment, 'ignore_errors', False)

        # <!-- sub-render included scenario
        errors_unused: set[str] = set()
        errors_undeclared: set[str] = set()

        # tag has specified variables, so lets "render"
        for name, value in variables.items():
            variable_template = f'{{$ {name} $}}'
            if variable_template not in scenario_content:
                errors_unused.add(name)
                continue

            scenario_content = scenario_content.replace(variable_template, str(value))

        # look for sub-variables that has not been rendered
        if not ignore_errors:
            if '{$' in scenario_content and '$}' in scenario_content:
                matches = re.finditer(r'\{\$ ([^$]+) \$\}', scenario_content, re.MULTILINE)

                for match in matches:
                    errors_undeclared.add(match.group(1))

            if len(errors_undeclared) + len(errors_unused) > 0:
                scenario_identifier = f'{feature}#{scenario}'
                buffer_error: list[str] = []
                if len(errors_unused) > 0:
                    errors_unused_message = '\n  '.join(errors_unused)
                    buffer_error.append(f'the following variables has been declared in scenario tag but not used in {scenario_identifier}:\n  {errors_unused_message}')
                    buffer_error.append('')

                if len(errors_undeclared) > 0:
                    errors_undeclared_message = '\n  '.join(errors_undeclared)
                    buffer_error.append(f'the following variables was used in {scenario_identifier} but was not declared in scenario tag:\n  {errors_undeclared_message}')
                    buffer_error.append('')

                message = '\n'.join(buffer_error)
                raise ValueError(message)

        # check if we have nested statements (`{% .. %}`), and render again if that is the case
        if '{%' in scenario_content and '%}' in scenario_content:
            environment = self.environment.overlay()
            environment.feature_file = feature_file
            template = environment.from_string(scenario_content)
            scenario_content = template.render()
        # // -->

        return scenario_content

    def filter_stream(self, stream: TokenStream) -> TokenStream | Iterable[Token]:  # type: ignore[return]  # noqa: PLR0912
        """Everything outside of `{% scenario ... %}` (and `{% if ... %}...{% endif %}`) should be treated as "data", e.g. plain text.

        Overloaded from `StandaloneTag`, must match method signature, which is not `Generator`, even though we yield
        the result instead of returning.
        """
        in_scenario = False
        in_block_comment = False
        in_condition = False
        in_variable = False

        variable_begin_pos = -1
        variable_end_pos = 0
        block_begin_pos = -1
        block_end_pos = 0
        source_lines = self._source.splitlines()

        for token in stream:
            if token.type == 'block_begin':
                if stream.current.value in self.tags:  # {% scenario ... %}
                    in_scenario = True
                    current_line = source_lines[token.lineno - 1].lstrip()
                    in_block_comment = current_line.startswith('#')
                    block_begin_pos = self._source.index(token.value, block_begin_pos + 1)
                elif stream.current.value in ['if', 'endif']:  # {% if <condition> %}, {% endif %}
                    in_condition = True

            if in_scenario:
                if token.type == 'block_end' and in_block_comment:
                    in_block_comment = False
                    block_end_pos = self._source.index(token.value, block_begin_pos)
                    token_value = self._source[block_begin_pos : block_end_pos + len(token.value)]
                    filtered_token = Token(token.lineno, 'data', token_value)
                elif in_block_comment:
                    continue
                else:
                    filtered_token = token
            elif in_condition:
                filtered_token = token
            else:
                if token.type == 'variable_begin':
                    # Find variable start in the source
                    variable_begin_pos = self._source.index(token.value, variable_begin_pos + 1)
                    in_variable = True
                    continue
                elif token.type == 'variable_end':
                    # Find variable end in the source
                    variable_end_pos = self._source.index(token.value, variable_begin_pos)
                    # Extract the variable definition substring and use as token value
                    token_value = self._source[variable_begin_pos : variable_end_pos + len(token.value)]
                    in_variable = False
                elif in_variable:  # Variable templates is yielded when the whole block has been processed
                    continue
                else:
                    token_value = token.value

                filtered_token = Token(token.lineno, 'data', token_value)

            yield filtered_token

            if token.type == 'block_end':
                if in_scenario:
                    in_scenario = False

                if in_condition:
                    in_condition = False


class MergeYamlTag(StandaloneTag):  # pragma: no cover
    tags: ClassVar[set[str]] = {'merge'}

    def preprocess(
        self,
        source: str,
        name: str | None,
        filename: str | None = None,
    ) -> str:
        self._source = source
        return cast('str', super().preprocess(source, name, filename))

    def render(self, filename: str, *filenames: str) -> str:
        buffer: list[str] = []

        files = [filename, *filenames]

        for file in files:
            merge_file = Path(file)

            # check if relative to parent feature file
            if not merge_file.exists():
                merge_file = (self.environment.source_file.parent / merge_file).resolve()

            if not merge_file.exists():
                raise FileNotFoundError(merge_file)

            merge_content = merge_file.read_text()

            if merge_content[0:3] != '---':
                buffer.append('---')

            buffer.append(merge_content)

        if self._source[0:3] != '---':
            buffer.append('---')

        return '\n'.join(buffer)

    def filter_stream(self, stream: TokenStream) -> TokenStream | Iterable[Token]:  # type: ignore[return]
        """Everything outside of `{% merge ... %}` should be treated as "data", e.g. plain text."""
        in_merge = False
        in_variable = False
        in_block_comment = False

        variable_begin_pos = -1
        variable_end_pos = 0
        block_begin_pos = -1
        block_end_pos = 0
        source_lines = self._source.splitlines()

        for token in stream:
            if token.type == 'block_begin' and stream.current.value in self.tags:
                in_merge = True
                current_line = source_lines[token.lineno - 1].lstrip()
                in_block_comment = current_line.startswith('#')
                block_begin_pos = self._source.index(token.value, block_begin_pos + 1)

            if not in_merge:
                if token.type == 'variable_end':
                    # Find variable end in the source
                    variable_end_pos = self._source.index(token.value, variable_begin_pos)
                    # Extract the variable definition substring and use as token value
                    token_value = self._source[variable_begin_pos : variable_end_pos + len(token.value)]
                    in_variable = False
                elif token.type == 'variable_begin':
                    # Find variable start in the source
                    variable_begin_pos = self._source.index(token.value, variable_begin_pos + 1)
                    in_variable = True
                else:
                    token_value = token.value

                if in_variable:
                    # While handling in-variable tokens, withhold values until
                    # the end of the variable is reached
                    continue

                filtered_token = Token(token.lineno, 'data', token_value)
            elif token.type == 'block_end' and in_block_comment:
                in_block_comment = False
                block_end_pos = self._source.index(token.value, block_begin_pos)
                token_value = self._source[block_begin_pos : block_end_pos + len(token.value)]
                filtered_token = Token(token.lineno, 'data', token_value)
            elif in_block_comment:
                continue
            else:
                filtered_token = token

            yield filtered_token

            if in_merge and token.type == 'block_end':
                in_merge = False


def get_keyvault_client(url: str) -> SecretClient:
    credential = ChainedTokenCredential(ManagedIdentityCredential(), AzureCliCredential())

    return SecretClient(vault_url=url, credential=credential)


def _get_metadata(content_type: str, name: str) -> str | None:
    try:
        start = content_type.index(f'{name}:') + len(name) + 1
    except ValueError:
        return None

    try:
        end = content_type.index(',', start)
    except ValueError:
        end = len(content_type)

    try:
        value = content_type[start:end]
    except IndexError:
        value = None

    return value


def _create_safe_file_and_parent(file: Path) -> Path:
    file.parent.mkdir(parents=True, exist_ok=True)
    file.parent.chmod(0o700)
    file.touch()
    file.chmod(0o600)

    return file


def _create_relative_path(root: Path, file: Path, *, no_suffix: bool = False) -> str:
    if no_suffix:
        file = file.with_suffix('')

    return file.as_posix().replace(root.as_posix(), '')[1:]


def _write_mqm_cert(
    root: Path,
    label: str,
    password: str | None,
    private_key: pkcs12.PKCS12PrivateKeyTypes | None,
    public_certificate: Certificate | None,
    additional_certificates: list[Certificate] | None,
    encryption_algorithm: KeySerializationEncryption,
) -> str:
    p12_file = _create_safe_file_and_parent(root / 'files' / f'{label}.p12')
    cms_file = p12_file.parent / f'{label}.kdb'

    if cms_file.exists():
        cms_file.unlink(missing_ok=True)

        for file_ext in ['rdb', 'sth']:
            cms_file.with_suffix(f'.{file_ext}').unlink(missing_ok=True)

    logger.debug('p12 file: %s', p12_file.as_posix())

    p12_data = pkcs12.serialize_key_and_certificates(
        name=label.encode('utf-8'),
        key=private_key,
        cert=public_certificate,
        cas=additional_certificates,
        encryption_algorithm=encryption_algorithm,
    )

    p12_file.write_bytes(p12_data)

    runmqakm_path = which('runmqakm')

    if runmqakm_path is None:
        message = "runmqakm could not be found, install IBM MQC Redist, and make sure that it's bin/ directory is added to PATH"
        raise ValueError(message)

    runmqakm_cmd: list[str] = [
        runmqakm_path,
        '-keydb',
        '-convert',
        '-new_format',
        'cms',
        '-old_format',
        'p12',
        '-db',
        p12_file.as_posix(),
        '-target',
        cms_file.as_posix(),
    ]

    if password is not None:
        runmqakm_cmd += [
            '-pw',
            password,
            '-stash',
        ]

    relative_file = cms_file.as_posix().replace(root.as_posix(), '')[1:]

    try:
        result = run_command(runmqakm_cmd, silent=True)

        if result.return_code != 0:
            for line in result.output or []:
                logger.error(line.decode('utf-8').strip())

            message = f'failed to create {relative_file}'
            raise ValueError(message)
    finally:
        p12_file.unlink()
        cms_file.with_suffix('.crl').unlink(missing_ok=True)

    for file in cms_file.parent.glob(f'{cms_file.stem}.*'):
        relative_cms_file = _create_relative_path(root, file)
        logger.info('wrote %s', relative_cms_file)

    return _create_relative_path(root, cms_file, no_suffix=True)


def _write_pem_private(root: Path, name: str, encryption_algorithm: KeySerializationEncryption, private_key: PrivateKeyTypes) -> str:
    private_key_file = _create_safe_file_and_parent(root / 'files' / f'{name}.key')

    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=encryption_algorithm,
    )

    private_key_file.write_bytes(private_key_pem)

    return _create_relative_path(root, private_key_file)


def _write_pem_public(root: Path, name: str, public_certificate: Certificate, additional_certificates: list[Certificate]) -> str:
    certificate_file = _create_safe_file_and_parent(root / 'files' / f'{name}.crt')

    certificate_data: list[bytes] = []

    for certificate in [public_certificate, *additional_certificates]:
        certificate_pem = certificate.public_bytes(encoding=serialization.Encoding.PEM)
        certificate_data.append(certificate_pem)

    certificate_file.write_bytes(b''.join(certificate_data))

    return _create_relative_path(root, certificate_file)


def _write_file(root: Path, content_type: str, encoded_content: str) -> str:
    file_name = _get_metadata(content_type, 'file')

    if file_name is None:
        message = 'could not find `file:` in content type'
        raise ValueError(message)

    file = _create_safe_file_and_parent(root / 'files' / file_name)

    complete = True

    if 'chunk' in content_type:  # 0 = chunk:N, 1 = chunks:T
        chunk_index = int(_get_metadata(content_type, 'chunk') or 0)
        chunk_count = int(_get_metadata(content_type, 'chunks') or 1)
        content_buffer: list[str] = []

        logger.debug('%% processing chunk %d of %d for %s', chunk_index + 1, chunk_count, file_name)

        if chunk_index == 0 and file.exists():
            file.unlink()

        try:
            content_buffer.append(file.read_text())
        except FileNotFoundError:
            if chunk_index > 0:  # pragma: no cover
                raise

        content_buffer.append(encoded_content)

        if chunk_index < chunk_count - 1:
            file.write_text(''.join(content_buffer))
            complete = False
        else:
            logger.debug('%% completed %s with chunk %d of %d', file_name, chunk_index + 1, chunk_count)
            encoded_content = ''.join(content_buffer)
            complete = True

    relative_file = _create_relative_path(root, file)

    if complete:
        content = b64decode(encoded_content)
        file.write_bytes(content)
        logger.debug('%% wrote %s (size %d)', relative_file, len(content))

    return relative_file


def _import_files(client: SecretClient, root: Path, secret: KeyVaultSecret) -> str:
    if secret.value is None:
        message = f'secret {secret.name} has no value'
        raise ValueError(message)

    file_keys = secret.value.split(',')

    for file_key in file_keys:
        file_secret = client.get_secret(file_key)
        if file_secret.properties.content_type is None:
            message = f'secret {file_key} has no content type'
            raise ValueError(message)

        if file_secret.value is None:
            message = f'secret {file_key} has no value'
            raise ValueError(message)

        logger.debug('%% keyvault key %s', file_key)

        # nested files
        if file_secret.properties.content_type.startswith('files'):
            conf_value = _import_files(client, root, file_secret)
            logger.debug('%% nested files configuration value is %s', conf_value)
        else:
            conf_value = _write_file(root, file_secret.properties.content_type, file_secret.value)

    return conf_value


def load_configuration(file: Path) -> Path:
    if not file.exists():
        message = f'{file.as_posix()} does not exist'
        raise ValueError(message)

    if file.suffix not in ['.yml', '.yaml']:
        message = 'configuration file must have file extension yml or yaml'
        raise ValueError(message)

    configuration = load_configuration_file(file)

    load_from_keyvault = ((configuration or {}).get('configuration', None) or {}).get('keyvault', None)

    if load_from_keyvault is not None:
        client = get_keyvault_client(load_from_keyvault)
        context_root = get_context_root()
        environment = configuration.get('configuration', {}).get('env', file.stem)

        loaded_keyvault_configuration, number_of_keyvault_secrets = load_configuration_keyvault(client, environment, context_root, filter_keys=None)
        keyvault_configuration = {'configuration': loaded_keyvault_configuration}

        configuration = merge_dicts(keyvault_configuration, configuration)

        logger.info('loaded %d secrets from keyvault %s', number_of_keyvault_secrets, load_from_keyvault)

    environment_lock_file = _create_safe_file_and_parent(file.parent / f'{file.stem}.lock{file.suffix}')

    with environment_lock_file.open('w') as fd:
        yaml.dump(configuration, fd, Dumper=IndentDumper.use_indentation(file), default_flow_style=False, sort_keys=False, allow_unicode=True)

    return file.with_name(f'{file.stem}.lock{file.suffix}')


def load_configuration_file(file: Path) -> dict:
    """Load a grizzly environment file and flatten the structure."""
    configuration: dict = {}

    environment = Environment(autoescape=False, extensions=[MergeYamlTag])
    environment.extend(source_file=file)
    loader = yaml.SafeLoader

    yaml_template = environment.from_string(file.read_text())
    yaml_content = yaml_template.render()

    yaml_configurations = list(yaml.load_all(yaml_content, Loader=loader))
    yaml_configurations.reverse()
    for yaml_configuration in yaml_configurations:
        configuration = merge_dicts(configuration, yaml_configuration)

    logger.debug('configuration: %r', configuration)

    return configuration


def filter_secrets(client: SecretClient, environment_filter: list[str]) -> dict[str, str]:
    secret_properties = client.list_properties_of_secrets()
    keys: dict[str, str] = {}

    # loop through all secrets to find the ones that match the environment filter
    for secret_property in secret_properties:
        if (
            secret_property.name is None
            or not secret_property.name.startswith('grizzly--')
            or 'noconf' in (secret_property.content_type or '')  # skip chunked secrets, we'll get them later
        ):
            continue

        _, target_environment, name = secret_property.name.split('--', 2)

        # hm, chunk suffix but no chunk in name?
        if '--' in name:
            logger.debug(f'secret {secret_property.name} with name {name} contains `--`, skipping')
            continue

        name = name.replace('-', '.')

        if target_environment not in environment_filter:
            continue

        keys.update({secret_property.name: name})

    return keys


def get_certificate_encryption_algorithm(client: SecretClient, password_key: str | None) -> tuple[KeySerializationEncryption, str | None]:
    # build encryption algorithm
    if password_key is not None:
        password_secret = client.get_secret(password_key)
        password = password_secret.value
    else:
        password = None

    if password is not None:
        encryption_algorithm = KeySerializationEncryptionBuilder(
            PrivateFormat.PKCS12,
            _key_cert_algorithm=PBES.PBESv1SHA1And3KeyTripleDESCBC,
        ).build(password.encode('utf-8'))
    else:
        encryption_algorithm = serialization.NoEncryption()

    return encryption_algorithm, password


def encode_certificate(client: SecretClient, root: Path, secret: KeyVaultSecret, content_type: str) -> str:
    assert secret.value is not None
    arguments: dict[str, str] = {}

    for part in secret.value.split(',', 1) + content_type.split(','):
        argument, value = part.split(':', 1)
        arguments.update({argument: value})

    cert_key = arguments['cert']
    cert_secret = client.get_secret(cert_key)

    if cert_secret.value is None:
        message = f'unable to download certificate secret {cert_key}'
        raise ValueError(message)

    if arguments.get('name') is None:
        name, _ = cert_key.split('-', 1)
        arguments.update({'name': name.lower()})

    certificate = b64decode(cert_secret.value)

    private_key, public_certificate, additional_certificates = pkcs12.load_key_and_certificates(data=certificate, password=None)

    encryption_algorithm, password = get_certificate_encryption_algorithm(client, arguments.get('pass'))

    # write files
    cert_format = arguments.get('format')
    if cert_format == 'pem-private':
        if private_key is None:
            message = f'could not find a private key in {cert_key}'
            raise ValueError(message)

        conf_value = _write_pem_private(root, arguments['name'], encryption_algorithm, private_key)
    elif cert_format == 'pem-public':
        if public_certificate is None:
            message = f'could not find a public certificate in {cert_key}'
            raise ValueError(message)

        conf_value = _write_pem_public(root, arguments['name'], public_certificate, additional_certificates)
    elif cert_format == 'mqm':
        conf_value = _write_mqm_cert(
            root,
            arguments['name'],
            password,
            cast('pkcs12.PKCS12PrivateKeyTypes | None', private_key),
            public_certificate,
            additional_certificates,
            encryption_algorithm,
        )
    else:
        message = f'{cert_format} is not a supported certificate format'
        raise ValueError(message)

    return conf_value


def encode_secret_value(client: SecretClient, root: Path, secret: KeyVaultSecret, secret_key: str) -> str:
    assert secret.value is not None
    assert secret.properties.content_type is not None

    content_type = secret.properties.content_type

    if content_type.startswith('files'):
        conf_value = _import_files(client, root, secret)
        if all(keyword in secret_key for keyword in ['mq', 'key']):
            conf_value = Path(conf_value).with_suffix('').as_posix()
    elif content_type.startswith('file:'):
        conf_value = _write_file(root, content_type, secret.value)
    elif content_type.startswith('format:') and secret.value.startswith('cert:'):
        conf_value = encode_certificate(client, root, secret, content_type)
    else:
        message = f'unknown content type for secret {secret_key}: {content_type}'
        raise ValueError(message)

    return conf_value


def load_configuration_keyvault(client: SecretClient, environment: str, root: Path, *, filter_keys: list[str] | None) -> tuple[dict, int]:
    keys = filter_secrets(client, environment_filter=['global', environment])
    configuration: dict = {}
    imported_secrets = 0

    # get the actual value for all secrets that matched environment filter
    for secret_key, conf_key in keys.items():
        secret = client.get_secret(secret_key)

        if filter_keys is not None and not any(conf_key.startswith(filter_key) for filter_key in filter_keys):
            continue

        content_type = secret.properties.content_type

        if secret.value is None:
            logger.error(f'! secret {secret_key} has no value, skipping')
            continue

        conf_value = secret.value

        no_conf = False

        if content_type is not None:
            no_conf = 'noconf' in content_type
            conf_value = encode_secret_value(client, root, secret, secret_key)

        if not no_conf:
            logger.debug('mapping %s to %s', conf_key, conf_value if content_type is not None else '******')
            configuration_branch = unflatten(conf_key, conf_value)
            configuration = merge_dicts(configuration_branch, configuration)
            imported_secrets += 1

    logger.debug('keyvault configuration: %r', configuration)

    return configuration, imported_secrets
