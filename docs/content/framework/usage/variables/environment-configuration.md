---
title: Environment configuration
---
@anchor framework.usage.variables.environment_configuration Environment configuration

It is possible to make the feature file environment agnostic by providing a `yaml` file containing a dictionary with a root node named `configuration`.
The environment configuration file can also be used to store credentials and other sensitive information that should not be under version control.

Internally `grizzly` will check if the environment variable `GRIZZLY_CONFIGURATION_FILE` is set and contains a valid environment configuration file. When using `grizzly-cli` you specify the file with `-e/--environment-file` which then will be set as a value for `GRIZZLY_CONFIGURATION_FILE`.

## Usage

In a feature file the dictionary can then be used by prefixing the path of a node under `configuration` with `$conf::<tree path to variable>$`.

Example:

```gherkin
Feature: application test
  Background: common configuration
    Given "1" users
    And spawn rate is "1" user per second
    And stop on first failure

  Scenario: frontend
    Given a user of type "RestApi" load testing "$conf::frontend.host$"
    ...

  Scenario: backend
    Given a user of type "RestApi" load testing "$conf::backend.host$"
    And set context variable "auth.user.username" to "$conf::backend.auth.user.username$"
    And set context variable "auth.user.password" to "$conf::backend.auth.user.password$"
```

This feature can now be run against a different environment just by creating a new environment configuration file with different values.

## Basic format

An example basic environment configuration file:

```yaml
configuration:
    frontend:
        host: https://www.example.com
    backend:
        host: https://backend.example.com
        auth:
            user:
                username: bob
                password: Who-the-f-is-alice
```

The only rule for any nodes under `configuration` is that it **must** be a dictionary, since the path to a value will be flattened.

## Advanced format

If you plan to run in multiple environments there could be cases that some environment variables are the same in all of the environments. If this is the case, it is possible
to merge multiple environment configuration files (`{% merge "<file1>" ["file2", ... ["fileN"]]}`), so that common values are only stored in one place.

The merging will take place from the bottom up, so contents in the source file where the `{% merge ... %}` statement is found, any duplicate keys in `fileN` will override the source values, `file2` will override those, and lastly `file1` will override those. So the first specified file to the `{% merge ... %}` statement will have highest precedence.

```
source < fileN < ... < file2 < file1
```

Example:

```yaml title="./common.yaml"
configuration:
    backend:
        auth:
            user:
                username: bob
                password: Who-the-f-is-alice
```

```yaml title="./staging.yaml"
{% merge "./common.yaml" %}
configuration:
    frontend:
        host: https://www.staging.example.com
    backend:
        host: https://backend.staging.example.com
```

```yaml title="./test.yaml"
{% merge "./common.yaml" %}
configuration:
    frontend:
        host: https://www.test.example.com
    backend:
        host: https://backend.test.example.com
```

For both environment files, this will produce the following configuration keys:
- `frontend.host`
- `backend.host`
- `backend.auth.user.username`
- `backend.auth.user.password`


## Azure Keyvault Secrets

It is possible to store environment configuration in an Azure Keyvault (secrets). The secrets in the keyvault needs to follow the naming convention `grizzly--<env>--<variable structure>` or `grizzly--global--<variable structure>`.

`<variable structure>` is the tree path to the value, where dots (`.`) is replaced with dash (`-`).

The values specified in the yaml file will have preceedence over keyvault values. To use keyvault, the key `configuration.keyvault` should be specified (keyvault URL):

```yaml
configuration:
    keyvault: https://<keyvault name>.vault.azure.net
```

Before starting `grizzly-cli`, you then have to login with `az login` so there are valid credentials to be used.

Keys with prefix `grizzly--global--` will be included in all environment files with `configuration.keyvault` defined. Keys with prefix `grizzly--<env>--` will be included in environment files where
`configuration.env` matches.

If this key is not in the environment file, it will use the name of the configuration file, e.g. `local.yaml` without `configuration.env`, will match keys where `<env>` is `local`.

The following environment configuration file:
```yaml title="environments/local.yaml"
configuration:
    keyvault: https://grizzly-dummy.vault.azure.net
```

With the following secrets in `grizzly-dummy` keyvault:
| key                                            | value  |
| ---------------------------------------------- | ------ |
| grizzly--global--authentication-admin-username | root   |
| grizzly--global--authentication-admin-password | hunter |
| grizzly--local--log-level                      | DEBUG  |

Will result in the following environment configuration:

```yaml title="environments/local.lock.yaml"
configuration:
    keyvault: https://grizzly-dummy.vault.azure.net
    authentication:
        admin:
            username: root
            password: hunter
    log:
        level: DEBUG
```

In your feature-files you can then reference these values, as normal environment configuration:

```gherkin
Given set context variable "auth.user.username" to "$conf::authentication.admin.username$"
And set context variable "auth.user.password" to "$conf::authentication.admin.password$"
```
