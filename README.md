# Grizzly - `/ˈɡɹɪzli/`

![grizzly logo](https://raw.githubusercontent.com/Biometria-se/grizzly/main/docs/assets/logo/grizzly_grasshopper_brown_256px.png)

Grizzly is a framework to be able to easily define load scenarios, and is mainly built on-top of two other frameworks:

> [Locust](https://locust.io): Define user behaviour with Python code, and swarm your system with millions of simultaneous users.

> [Behave](https://behave.readthedocs.io/): Uses tests written in a natural language style, backed up by Python code.

[Locust](https://en.wikipedia.org/wiki/Locust) are a group of certain species of short-horned grasshoppers in the family Arcididae that have a swarming phase.

The name grizzly was chosen based on the grasshopper [Melanoplus punctulatus](https://en.wikipedia.org/wiki/Melanoplus_punctulatus), also known as __grizzly__ spur-throat grasshopper. This species [prefers living in trees](https://www.sciencedaily.com/releases/2005/07/050718234418.htm) over grass, which is a hint to [Biometria](https://www.biometria.se/)<sup>1</sup>, where `grizzly` originally was created.

<sup>1</sup> _Biometria is a member owned and central actor within the swedish forestry that performs unbiased measurement of lumber flowing between forest and industry so that all of Swedens forest owners can feel confident selling their lumber._

## Documentation

More detailed documentation can be found [here](https://biometria-se.github.io/grizzly) and the easiest way to get started is to check out the [example](https://biometria-se.github.io/grizzly/example/).

## Description

`behave` is <del>abused</del> used for being able to define `locust` load test scenarios using [gherkin](https://cucumber.io/docs/gherkin). A feature can contain more than one scenario and all scenarios will run in parallell.

```gherkin
Feature: Rest API endpoint testing
  Background: Common properties for all scenarios
    Given "2" users
    And spawn rate is "2" user per second
    And stop on first failure

  Scenario: Authorize
    Given a user of type "RestApi" sending requests to "https://api.example.com"
    And repeat for "2" iterations
    And wait time inbetween requests is random between "0.1" and "0.3" seconds
    And value for variable "AtomicDate.called" is "now | format='%Y-%m-%dT%H:%M:%S.00Z' timezone=UTC"
    And value for variable "callback_endpoint" is "none"
    Then post request with name "authorize" from endpoint "/api/v1/authorize?called={{ AtomicDate.called }}"
        """
        {
            "username": "test",
            "password": "password123",
            "callback": "/api/v1/user/test"
        }
        """
    Then save response payload "$.callback" in variable "callback_endpoint"

    Then get request with name "user info" from endpoint "{{ callback_endpoint }}"
    When response payload "$.user.name" is not "Test User" stop user
```

This makes it possible to implement load test scenarios without knowing python or how to use `locust`.

## Features

A number of features that we thought `locust` was missing out-of-the-box has been implemented in `grizzly`.

### Test data

Support for synchronous handling of test data (variables). This is extra important when running `locust` distributed and there is a need for each worker and user to have unique test data, that cannot be re-used.

The solution is heavily inspired by [Karol Brejnas locust experiments - feeding the locust](https://medium.com/locust-io-experiments/locust-experiments-feeding-the-locusts-cf09e0f65897). A producer is running on the master (or local) node and keeps track of what has been sent to the consumer running on a worker (or local) node. The two communicates over a seperate [ZeroMQ](https://zeromq.org) session.

When the consumer wants new test data, it sends a message to the server that it is available and for which scenario it is going to run. The producer then responds with unique test data that can be used.

### Statistics

Listeners for both InfluxDB and Azure Application Insights are included. The later is more or less [`appinsights_listener.py`](https://github.com/SvenskaSpel/locust-plugins/blob/master/locust_plugins/appinsights_listener.py), from the good guys at [Svenska Spel](https://github.com/SvenskaSpel), but with typing.

They are useful when history of test runs is needed, or when wanting to correlate load tests with other events in the targeted environment.

### Load test users

`locust` comes with a simple user for loading an HTTP(S) endpoint and due to the nature of how the integration between `behave` and `locust` works, it is not possible to use `locust` provided users, even for HTTP(S) targets.

* `RestApiUser`: send requests to REST API endpoinds, supports authentication with username+password or client secret
* `ServiceBusUser`: send to and receive from Azure Service Bus queues and topics
* `MessageQueueUser`: send and receive from IBM MQ queues
* `SftpUser`: send and receive files from an SFTP-server
* `BlobStorageUser`: send files to Azure Blob Storage<sup>2</sup>

<sup>2</sup> A pull request for functionality in the other direction is appreciated!

### Request log

All failed requests are logged to a file which includes both header and body, both for request and response.

## Installation

```bash
pip3 install grizzly-loadtester
pip3 install grizzly-loadtester-cli
```

Do not forget to try the [example](https://biometria-se.github.io/grizzly/example/) which also serves as a boilerplate scenario project.

## Development

The easiest way to start contributing to this project is to have [Visual Studio Code](https://code.visualstudio.com/) (with "Remote - Containers" extension) and [docker](https://www.docker.com/) installed. The project comes with a `devcontainer`, which encapsulates everything needed for a development environment.

It is also possible to use a python virtual environment where `requirements.txt` and `requirements-dev.txt` is installed, and also preferbly the IBM MQ client dependencies and `requirements-extras.txt`.
