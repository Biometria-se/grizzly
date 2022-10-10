---
title: Environment configuration
---
@anchor framework.usage.variables.environment_configuration Environment configuration

It is possible to make the feature file environment agnostic by providing a `yaml` file containing a dictionary with a root node named `configuration`.
The environment configuration file can also be used to store credentials and other sensitive information that should not be under version control.

Internally `grizzly` will check if the environment variable `GRIZZLY_CONFIGURATION_FILE` is set and contains a valid environment configuration file. When using `grizzly-cli` you specify the file with `-e/--environment-file` which then will be set as a value for `GRIZZLY_CONFIGURATION_FILE`.

## Format

An example environment configuration file:

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
