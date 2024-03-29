# Grizzly - `/ˈɡɹɪzli/`

<img align="right" src="https://raw.githubusercontent.com/Biometria-se/grizzly/main/docs/content/assets/logo/grizzly_grasshopper_brown_256px.png" alt="grizzly logo">
<span>

###### Framework

![PyPI - License](https://img.shields.io/pypi/l/grizzly-loadtester?style=for-the-badge)
![PyPI](https://img.shields.io/pypi/v/grizzly-loadtester?style=for-the-badge)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/grizzly-loadtester?style=for-the-badge)

###### Command Line Interface

![PyPI - License](https://img.shields.io/pypi/l/grizzly-loadtester-cli?style=for-the-badge)
![PyPI](https://img.shields.io/pypi/v/grizzly-loadtester-cli?style=for-the-badge)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/grizzly-loadtester-cli?style=for-the-badge)

###### Editor Support / Language Server
![PyPI - License](https://img.shields.io/pypi/l/grizzly-loadtester-ls?style=for-the-badge)
![PyPI](https://img.shields.io/pypi/v/grizzly-loadtester-ls?style=for-the-badge)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/grizzly-loadtester-ls?style=for-the-badge)

###### Editor Support / Visual Studio Code Extension
![GitHub License](https://img.shields.io/github/license/Biometria-se/grizzly-lsp?style=for-the-badge)
![Visual Studio Marketplace Version (including pre-releases)](https://img.shields.io/visual-studio-marketplace/v/biometria-se.grizzly-loadtester-vscode?style=for-the-badge)
![Visual Studio Marketplace Release Date](https://img.shields.io/visual-studio-marketplace/release-date/biometria-se.grizzly-loadtester-vscode?style=for-the-badge)
</span>

**Grizzly is a framework to be able to easily define load scenarios, and is primarily built on-top of two other frameworks.**

> [Locust](https://locust.io): Define user behaviour with Python code, and swarm your system with millions of simultaneous users.

> [Behave](https://behave.readthedocs.io/): Uses tests written in a natural language style, backed up by Python code.

**`behave` is <del>ab</del>used for being able to define `locust` load test scenarios using [gherkin](https://cucumber.io/docs/gherkin). A feature can contain more than one scenario and all scenarios will run in parallell. This makes it possible to implement load test scenarios without knowing python or how to use `locust`.**

[Locust](https://en.wikipedia.org/wiki/Locust) are a group of certain species of short-horned grasshoppers in the family Arcididae that have a swarming phase.

The name grizzly was chosen based on the grasshopper [Melanoplus punctulatus](https://en.wikipedia.org/wiki/Melanoplus_punctulatus), also known as __grizzly__ spur-throat grasshopper. This species [prefers living in trees](https://www.sciencedaily.com/releases/2005/07/050718234418.htm) over grass, which is a hint to [Biometria](https://www.biometria.se/)<sup>1</sup>, where `grizzly` originally was created.

<sup>1</sup> _Biometria is a member owned and central actor within the swedish forestry that performs unbiased measurement of lumber flowing between forest and industry so that all of Swedens forest owners can feel confident selling their lumber._

## Documentation

More detailed documentation can be found [here](https://biometria-se.github.io/grizzly) and the easiest way to get started is to check out the [example](https://biometria-se.github.io/grizzly/example/).


## Features

A number of features that we thought `locust` was missing out-of-the-box has been implemented in `grizzly`.

### Test data

Support for synchronous handling of test data (variables). This is extra important when running `locust` distributed and there is a need for each worker and user to have unique test data, that cannot be re-used.

The solution is heavily inspired by [Karol Brejnas locust experiments - feeding the locust](https://medium.com/locust-io-experiments/locust-experiments-feeding-the-locusts-cf09e0f65897). A producer is running on the master (or local) node and keeps track of what has been sent to the consumer running on a worker (or local) node. The two communicates over a dedicated [ZeroMQ](https://zeromq.org) connection.

When the consumer wants new test data, it sends a message to the server that it is available and for which scenario it is going to run. The producer then responds with unique test data that can be used.

### Statistics

Listeners for both InfluxDB and Azure Application Insights are included. The later is more or less [`appinsights_listener.py`](https://github.com/SvenskaSpel/locust-plugins/blob/master/locust_plugins/appinsights_listener.py), from the good guys at [Svenska Spel](https://github.com/SvenskaSpel), but with typing.

They are useful when history of test runs is needed, or when wanting to correlate load tests with other events in the targeted environment.

### Load test users

`locust` comes with a simple user for loading an HTTP(S) endpoint and due to the nature of how the integration between `behave` and `locust` works in `grizzly`, it is not possible to directly use `locust.user.users` provided users, even for HTTP(S) targets.

* `RestApiUser`: send requests to REST API endpoinds, supports authentication with username+password or client secret
* `ServiceBusUser`: send to and receive from Azure Service Bus queues and topics
* `MessageQueueUser`: send and receive from IBM MQ queues
* `BlobStorageUser`: send and receive files to Azure Blob Storage
* `IotHubUser`: send/put files to Azure IoT Hub

### Request log

All failed requests are logged to a file which includes both header and body, both for request and response.

## Installation

```bash
pip3 install grizzly-loadtester
pip3 install grizzly-loadtester-cli
```

Do not forget to try the [example](https://biometria-se.github.io/grizzly/example/) which also serves as a boilerplate scenario project, or create a new grizzly project with:

```bash
grizzly-cli init my-grizzly-project
```

## Development

The easiest way to start contributing to this project is to have [Visual Studio Code](https://code.visualstudio.com/) (with "Remote - Containers" extension) and [docker](https://www.docker.com/) installed. The project comes with a `devcontainer`, which encapsulates everything needed for a development environment.

It is also possible to use a python virtual environment, but then you would have to manually download and install IBM MQ libraries, and install `grizzly` dependencies.

```bash
sudo mkdir /opt/mqm && cd /opt/mqm && wget https://ibm.biz/IBM-MQC-Redist-LinuxX64targz -O - | tar xzf -
export LD_LIBRARY_PATH="/opt/mqm/lib64:${LD_LIBRARY_PATH}"
cd ~/
git clone https://github.com/Biometria-se/grizzly.git
cd grizzly/
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .[dev,ci,mq,docs]
```
