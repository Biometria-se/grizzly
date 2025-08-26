# async-messaged

Can be seen as an integration gateway for different messaging types. It has support for:
- IBM MQ
- Azure Service Bus

A client communicates with `async-messaged` over an zmq router socket with the [request-reply request pattern](https://rfc.zeromq.org/spec/28/).

This was needed [for grizzly] due to `gevent` not playing nice with native libraries, which both of these libraries have (or had in the past).
