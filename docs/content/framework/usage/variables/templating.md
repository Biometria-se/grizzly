---
title: Templating
---
@anchor framework.usage.variables.templating
# Templating

`grizzly` has support for templating in both step expression variables (most) and request payload, with the templating backend [Jinja2](https://jinja.palletsprojects.com/en/3.0.x/).

## Request payload

Request payload is treated as complete Jinja2 templates and has full support for any Jinja2 features. Request payload files **must** be stored in `./features/requests` and are referenced in a feature file as a relative path to that directory.

```plain
.
└── features
    ├── load-test.feature
    └── requests
        └── load-test
            └── request.j2.json
```

Consider that `load-test.feature` contains the following steps:

```gherkin
Feature: templating example
  Background: common settings for all scenarios
    Given "1" user
    And spawn rate is "1" user per second
    And stop on first failure

  Scenario: example
    Given a user of type "RestApi" load testing "https://localhost"
    And repeat for "3" iterations
    And value for variable "AtomicIntegerIncrementer.items" is "1 | step=3"
    Then post request "load-test/request.j2.json" with name "template-request" to endpoint "/api/v1/test"
```

`request.j2.json` is a full Jinja2 template which will be rendered before the request is sent. The reason for this is that testdata variables can be used in the template, and these can change for each request.

If `request.j2.json` contains the following:

```json
[
	{%- for n in range(AtomicIntegerIncrementer.items) %}
		{
			"item": {{ n }},
			"name": "item-{{ n }}"
		}
		{%- if n < AtomicIntegerIncrementer.items - 1 %},{%- endif %}
	{%- endfor %}
]
```

Since the scenario has been setup to run for `3` iterations with `1` user and assumed that we run it locally, or distributed with one worker node, the scenario will run three times.

The first post request to `/api/v1/test` will have the following payload:

```json
[
  {
    "item": 0,
    "name": "item-0"
  }
]
```

The second post request:

```json
[
  {
    "item": 0,
    "name": "item-0"
  },
  {
    "item": 1,
    "name": "item-1"
  },
  {
    "item": 2,
    "name": "item-2"
  },
  {
    "item": 3,
    "name": "item-3"
  }
]
```

The third post request:

```json
[
  {
    "item": 0,
    "name": "item-0"
  },
  {
    "item": 1,
    "name": "item-1"
  },
  {
    "item": 2,
    "name": "item-2"
  },
  {
    "item": 3,
    "name": "item-3"
  },
  {
    "item": 4,
    "name": "item-4"
  },
  {
    "item": 5,
    "name": "item-5"
  },
  {
    "item": 6,
    "name": "item-6"
  }
]
```

## Step expression

Most step expressions also support templating for their variables, for example:

```gherkin
And set context variable "auth.user.username" to "$conf::backend.auth.user.username"
And set context variable "auth.refresh_time" to "{{ AtomicIntegerIncrementer.refresh_time }}"
And repeat for "{{ iterations * 0.25 }}"
And save statistics to "influxdb://$conf::statistics.username:$conf::statistics.password@{{ influxdb_host }}/$conf::statistics.database"
And ask for value of variable "initial_id"
And value for variable "AtomicIntegerIncrementer.id1" is "{{ initial_id }}"
And value for variable "AtomicIntegerIncrementer.id2" is "{{ initial_id }}"
Then put request with name "example-{{ initial_id }}" to "/api/v{{ initial_id }}/test"
    """
    {
        "test": {
            "value": "{{ initial_id }}"
        }
    }
    """
```

