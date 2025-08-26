"""Functionality for `grizzly-cli keyvault ...`."""

from __future__ import annotations

import logging
import re
from base64 import b64encode
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError, ServiceRequestError

from grizzly_cli import register_parser
from grizzly_cli.argparse.bashcompletion import BashCompletionTypes
from grizzly_cli.utils import IndentDumper, chunker, flatten, logger, merge_dicts, unflatten
from grizzly_cli.utils.configuration import get_context_root, get_keyvault_client, load_configuration_file, load_configuration_keyvault

if TYPE_CHECKING:  # pragma: no cover
    from argparse import ArgumentParser as CoreArgumentParser
    from argparse import Namespace as Arguments

    from azure.keyvault.secrets import SecretClient

    from grizzly_cli.argparse import ArgumentSubParser

# disable azure.identity warning logs if authentication fails
azure_logger = logging.getLogger('azure')
azure_logger.setLevel(logging.ERROR)


KEYWORDS = ['password', 'secret', 'key', 'cert', 'token']
COMMON_FALSE_POSITIVES = ['RootManageSharedAccessKey']
KEYVAULT_MAX_SIZE = 25600 - 100  # 100 bytes buffer


@dataclass
class KeyvaultSecretHolder:
    name: str
    content_type: str | None
    value: str


@register_parser()
def create_parser(sub_parser: ArgumentSubParser) -> None:
    # grizzly-cli keyvault
    keyvault_parser = sub_parser.add_parser('keyvault', description=('grizzly keyvault integration'))

    keyvault_parser.add_argument(
        '-f',
        '--file',
        dest='env_file',
        type=BashCompletionTypes.File('*.yaml', '*.yml', missing_ok=True),
        required=True,
        help='path to the grizzly environment configuration file',
    )

    keyvault_parser.add_argument(
        '--vault-name',
        dest='keyvault',
        metavar='URL',
        type=str,
        help='keyvault url',
        default=None,
        required=False,
    )

    if keyvault_parser.prog != 'grizzly-cli keyvault':
        keyvault_parser.prog = 'grizzly-cli keyvault'

    sub_parser = keyvault_parser.add_subparsers(dest='subcommand')

    create_import_parser(sub_parser)
    create_export_parser(sub_parser)
    create_diff_parser(sub_parser)


def add_generic_arguments(parser: CoreArgumentParser) -> None:
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='enable verbose output',
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='do not write to keyvault',
    )

    parser.add_argument(
        '-k',
        '--key',
        action='append',
        dest='keys',
        type=str,
        required=False,
        help='filter on specified keys',
    )


def create_diff_parser(sub_parser: ArgumentSubParser) -> None:
    # grizzly-cli keyvault diff
    diff_parser = sub_parser.add_parser('diff', description='compare two environment configuration files')

    diff_parser.add_argument('orig_file', nargs=None, type=BashCompletionTypes.File('*.yaml', '*.yml'), help='file to compare -f/--file with')

    if diff_parser.prog != 'grizzly-cli keyvault diff':
        diff_parser.prog = 'grizzly-cli keyvault diff'


def create_import_parser(sub_parser: ArgumentSubParser) -> None:
    # grizzly-cli keyvault import
    import_parser = sub_parser.add_parser('import', description=('import grizzly environment configuration from keyvault secrets'))

    add_generic_arguments(import_parser)

    if import_parser.prog != 'grizzly-cli keyvault import':
        import_parser.prog = 'grizzly-cli keyvault import'


def create_export_parser(sub_parser: ArgumentSubParser) -> None:
    # grizzly-cli keyvault export
    export_parser = sub_parser.add_parser('export', description=('export grizzly environment configuration to keyvault secrets'))

    export_parser.add_argument(
        '-g',
        '--global',
        dest='global_configuration',
        metavar='KEY',
        action='append',
        default=[],
        type=str,
        required=False,
        help='environment configuration keys that are global',
    )

    add_generic_arguments(export_parser)

    if export_parser.prog != 'grizzly-cli keyvault export':
        export_parser.prog = 'grizzly-cli keyvault export'


def _keyvault_normalize(value: str) -> str:
    return re.sub(r'[^a-zA-Z0-9]', '-', value)


def encode_mq_certificate(root: Path, environment: str, key: str, base_cert_name: str) -> list[KeyvaultSecretHolder]:
    certificate_files = root.glob(f'{base_cert_name}.*')
    secrets: list[KeyvaultSecretHolder] = []

    references: list[str] = []

    for certificate_file in certificate_files:
        key_name = f'grizzly--{environment}--{_keyvault_normalize(certificate_file.name)}'
        references.append(key_name)
        secrets.extend(encode_file(key_name, certificate_file, no_conf=True))

    secrets.append(
        KeyvaultSecretHolder(
            name=key,
            content_type='files',
            value=','.join(references),
        ),
    )

    return secrets


def encode_file(key: str, file: str | Path, *, no_conf: bool = False) -> list[KeyvaultSecretHolder]:
    secrets: list[KeyvaultSecretHolder] = []
    if isinstance(file, str):
        file = Path(file)

    value = b64encode(file.read_bytes()).decode('utf-8')

    content_type_extra = ''

    if no_conf:
        content_type_extra = ',noconf'

    if len(value) > KEYVAULT_MAX_SIZE:
        references: list[str] = []
        chunks = chunker(value, KEYVAULT_MAX_SIZE)
        for index, chunk in enumerate(chunks):
            chunk_key = f'{key}--{index}'
            references.append(chunk_key)

            logger.debug(f'% creating chunk {index + 1} of {len(chunks)} in key {chunk_key}')

            secrets.append(
                KeyvaultSecretHolder(
                    name=chunk_key,
                    content_type=f'file:{file.name},chunk:{index},chunks:{len(chunks)}{content_type_extra}',
                    value=chunk,
                )
            )

        secrets.append(
            KeyvaultSecretHolder(
                name=key,
                content_type=f'files{content_type_extra}',
                value=','.join(references),
            )
        )
    else:
        secrets.append(
            KeyvaultSecretHolder(
                name=key,
                content_type=f'file:{file.name}{content_type_extra}',
                value=value,
            )
        )

    return secrets


def _should_export(key: str, secret: Any) -> bool:
    should: list[bool] = []

    if key == 'keyvault' and secret.endswith('.vault.azure.net'):
        return False

    for keyword in KEYWORDS:
        key_should = keyword in key.lower() and f'.{keyword}.' not in key
        secret_should = isinstance(secret, str) and keyword in secret.lower() and secret not in COMMON_FALSE_POSITIVES

        # if key should be exported, but not the secret and the secret is not a string... it shouldn't
        if key_should and not secret_should:
            key_should = isinstance(secret, str)

        should.append(key_should or secret_should)

    return any(should)


def _determine_environment(global_configuration: list[str], environment: str, key: str) -> str:
    return 'global' if any(key.startswith(keyword) for keyword in global_configuration) else environment


def _build_key_name(environment: str, key: str) -> str:
    return f'grizzly--{environment}--{_keyvault_normalize(key)}'


def _dict_to_yaml(file: Path, content: dict, *, indentation: Path | int) -> None:
    file.write_text('')  # make sure file is empty

    with file.open('w') as fd:
        yaml.dump(content, fd, Dumper=IndentDumper.use_indentation(indentation), default_flow_style=False, sort_keys=False, allow_unicode=True)


def _extract_metadata(env_file: str) -> tuple[str, str | None, dict]:
    file = Path(env_file)
    configuration = load_configuration_file(file).get('configuration', {})

    keyvault = configuration.get('keyvault', None)

    configuration = flatten(configuration)

    logger.debug('flatten configuration: %r', configuration)

    return (configuration.get('env', None) or file.stem, keyvault, configuration)


def diff(left_file_name: str, right_file_name: str) -> int:
    left_config_file = Path(left_file_name)
    right_config_file = Path(right_file_name)

    if not left_config_file.exists():
        message = f'environment configuration file {left_config_file.as_posix()} does not exist'
        raise ValueError(message)

    if not right_config_file.exists():
        message = f'environment configuration file {right_config_file.as_posix()} does not exist'
        raise ValueError(message)

    left_config = flatten(load_configuration_file(right_config_file)['configuration'])
    right_config = flatten(load_configuration_file(left_config_file)['configuration'])

    logger.info(f'- {right_config_file.as_posix()}')
    logger.info(f'+ {left_config_file.as_posix()}')
    logger.info('')

    diffed_keys: set[str] = set()

    for key, value in left_config.items():
        if key not in right_config or right_config.get(key, None) != value:
            logger.error(f'- {key}: {value} != {right_config.get(key, None)}')
            diffed_keys.add(key)

    for key, value in right_config.items():
        if key not in left_config or (left_config.get(key, None) != value and key not in diffed_keys):
            logger.error(f'+ {key}: {left_config.get(key, None)} != {value}')

    return 0


def keyvault_import(client: SecretClient, environment: str, args: Arguments, root: Path, configuration: dict) -> int:
    # unflatten existing configuration
    configuration_unflatten: dict = {}

    original_key_count = len(configuration)

    for conf_key, conf_value in configuration.items():
        configuration_branch = unflatten(conf_key, conf_value)
        configuration_unflatten = merge_dicts(configuration_branch, configuration_unflatten)

    if len(configuration_unflatten) < 1:
        logger.error('environment file %s did not contain any configuration', args.env_file)
        return 1

    configuration = configuration_unflatten

    logger.debug('exploded configuration: %r', configuration)

    keyvault_configuration, imported_secrets = load_configuration_keyvault(client, environment, root, filter_keys=args.keys)

    configuration = merge_dicts(keyvault_configuration, configuration)

    logger.debug('merged configuration: %r', configuration)

    environment_file = Path(args.env_file)

    if not args.dry_run:  # do not rewrite environment file on dry-run
        flatten_configuration = flatten(configuration)
        written_keys = len(flatten_configuration) - original_key_count
        unsafe_environment_file = environment_file.rename(environment_file.with_suffix(f'.unsafe{environment_file.suffix}'))
        _dict_to_yaml(environment_file, {'configuration': configuration}, indentation=environment_file)
    else:
        written_keys = 0
        unsafe_environment_file = environment_file.with_suffix(f'.unsafe{environment_file.suffix}')

    logger.info(
        '\nimported %d secrets from %s and wrote %d new keys to %s, saved original in %s',
        imported_secrets,
        client.vault_url,
        written_keys,
        environment_file.as_posix(),
        unsafe_environment_file.as_posix(),
    )

    return 0


def upsert_secrets(client: SecretClient, secrets: list[KeyvaultSecretHolder], *, dry_run: bool) -> int:
    created_secrets_count = 0

    for secret in secrets:
        # check if secret already exists
        try:
            current_value = client.get_secret(secret.name)
            if current_value.value != secret.value or current_value.properties.content_type != secret.content_type:
                raise ResourceNotFoundError

            # it exists with the same value, skip
            continue
        except ResourceNotFoundError:  # great, import it into the keyvault
            pass

        logger.debug(f'% keyvault secret {secret.name} with content type {secret.content_type}')

        if not dry_run:
            client.set_secret(secret.name, secret.value, content_type=secret.content_type)

        created_secrets_count += 1

    return created_secrets_count


def prepare_secrets(
    client: SecretClient,
    environment: str,
    root: Path,
    configuration: dict,
    *,
    filter_keys: list[str] | None,
    global_configuration: list[str],
) -> tuple[dict, list[KeyvaultSecretHolder]]:
    secrets: list[KeyvaultSecretHolder] = []
    safe_configuration: dict = {}

    for key, secret in configuration.items():
        if not _should_export(key, secret):
            safe_configuration.update({key: secret})
            continue

        if filter_keys is not None and key not in filter_keys:
            continue

        key_environment = _determine_environment(global_configuration, environment, key)
        key_name = _build_key_name(key_environment, key)

        if secret.startswith('cert:'):
            if '#' in secret:
                secret, content_type = secret.split('#', 1)  # noqa: PLW2901
            else:
                content_type = None

            if 'pass:' in secret:
                _, password_ref = secret.split(',', 1)
                _, password_key = password_ref.split(':', 1)
                try:
                    client.get_secret(password_key)
                except ResourceNotFoundError:
                    message = f'key {password_key} referenced in value for {key} does not exist'
                    raise ValueError(message) from None

            secrets.append(
                KeyvaultSecretHolder(
                    name=key_name,
                    content_type=content_type,
                    value=secret,
                )
            )
        elif 'file' in key:
            if 'mq' in key:
                secrets.extend(encode_mq_certificate(root, key_environment, key_name, secret))
            else:
                secrets.extend(encode_file(key_name, secret))
        else:
            secrets.append(
                KeyvaultSecretHolder(
                    name=key_name,
                    content_type=None,
                    value=secret,
                )
            )

    return safe_configuration, secrets


def keyvault_export(client: SecretClient, environment: str, args: Arguments, root: Path, configuration: dict) -> int:
    """From grizzly to keyvault.

    If the specified secret starts with `cert:`, it indicates that it should reference a keyvault certificate (the name). This value
    can have metadata (secret content type) appended after the actual value the `#` separator. If the output certificate should be
    password protected, `pass:` should reference a keyvault secret which contains the password.

    `cert:<keyvault certificate name>[,pass:<keyvault secret name for password>][#format:[mqm|pem-public|pem-private]]`

    Supported certificate output formats:
    - `mqm`, MQ CMS keystore
    - `pem-public`, public certificate in PEM format
    - `pem-private`, private key in PEM format

    If the configuration key contains `file` in the path, the configuration value will be base64 encoded and, optionally, chunked into
    keyvault secrets. If the path also contains `mq`, all MQ keystore/CMS files will be encoded, chunked and then references to the
    actual configuration key.
    """
    environment_file = Path(args.env_file)

    safe_configuration, secrets = prepare_secrets(client, environment, root, configuration, filter_keys=args.keys, global_configuration=args.global_configuration)

    created_secrets_count = upsert_secrets(client, secrets, dry_run=args.dry_run)
    already_exists = len(secrets) - created_secrets_count

    if not args.dry_run:
        unsafe_environment_file = environment_file.rename(environment_file.with_suffix(f'.unsafe{environment_file.suffix}'))
        safe_yaml_configuration: dict = {}

        for key, value in safe_configuration.items():
            safe_yaml_configuration = merge_dicts(safe_yaml_configuration, unflatten(key, value))

        if safe_yaml_configuration.get('keyvault', None) is None:
            safe_yaml_configuration.update({'keyvault': client.vault_url})

        safe_yaml_configuration = {'configuration': safe_yaml_configuration}

        # remove contents in safe file, before writing the safe configuration
        _dict_to_yaml(environment_file, safe_yaml_configuration, indentation=unsafe_environment_file)
    else:
        unsafe_environment_file = environment_file.with_suffix(f'.unsafe{environment_file.suffix}')

    logger.info(
        f'created {created_secrets_count} ({already_exists} already existed) secrets in keyvault {client.vault_url} '
        f'and saved the safe environment configuration in {environment_file.as_posix()}',
    )
    logger.warning(f'! the unsafe environment configuration is still present in {unsafe_environment_file.as_posix()}')

    return 0


def keyvault(args: Arguments) -> int:
    grizzly_context_root = get_context_root()

    with suppress(AttributeError):
        if args.verbose or args.dry_run:
            logger.setLevel(logging.DEBUG)

    args_keyvault = f'https://{args.keyvault}.vault.azure.net' if args.keyvault is not None else None

    env_file = Path(args.env_file)
    if args.subcommand == 'import' and not env_file.exists():
        if args.keyvault is None:
            message = f'--vault-name not specified and environment configuration file {args.env_file} does not exist'
            raise ValueError(message)

        _dict_to_yaml(env_file, {'configuration': {'keyvault': args_keyvault}}, indentation=2)

    environment, keyvault, configuration = _extract_metadata(args.env_file)

    if keyvault is None and args.keyvault is None:
        message = 'keyvault not specified in environment configuration file and no keyvault specified on the command line'
        raise ValueError(message)

    keyvault = keyvault or args_keyvault

    if keyvault is None:
        message = 'keyvault not specified, please specify a keyvault'
        raise ValueError(message)

    client = get_keyvault_client(keyvault)

    try:
        if args.subcommand == 'import':  # from keyvault
            return keyvault_import(client, environment, args, grizzly_context_root, configuration)

        if args.subcommand == 'export':  # to keyvault
            return keyvault_export(client, environment, args, grizzly_context_root, configuration)

        if args.subcommand == 'diff':
            return diff(args.env_file, args.orig_file)

        message = f'unknown subcommand {args.subcommand}'
        raise ValueError(message)
    except ClientAuthenticationError:
        logger.error('authentication failed, if you are running from a resource which does not have a managed identity then you must run `az login` first.')
        return 1
    except ServiceRequestError:
        logger.error(f'{client.vault_url} does not resolve to an azure keyvault')
        return 1
    except ValueError:
        raise
