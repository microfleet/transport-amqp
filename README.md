# Microservice Utils

Contains rabbitmq-based transport for establishing a net of loosely coupled microservices with a simple rpc-style
calling interface.

[![npm version](https://badge.fury.io/js/%40microfleet%2Fcore.svg)](https://badge.fury.io/js/%40microfleet%2Fcore)
[![Build Status](https://semaphoreci.com/api/v1/makeomatic/transport-amqp/branches/master/shields_badge.svg)](https://semaphoreci.com/makeomatic/transport-amqp)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg?style=flat-square)](https://github.com/semantic-release/semantic-release)

## Install

`npm i @microfleet/transport-amqp -S`

Heavily relies on `dropbox/amqp-coffee` & `@microfleet/amqp-coffee` lib for establishing communication to rabbitmq

## Usage

```js
const AMQPTransport = require('@microfleet/transport-amqp');

const amqp = new AMQPTransport(config);

amqp.connect().then((amqpInstance) => {
  // amqp === amqpInstance
  // we are connected, do things here
});
```

In a more generic case, you want to use `.connect(config, messageHandler)` helper, which would create queues and bind them to exchanges for you
based on the provided configuration.
Consider the following example:

```js
const AMQPTransport = require('@microfleet/transport-amqp');

// message routers
function router(message, headers, actions, next) {
  switch (headers.routingKey) {
    case 'very.important.route':
      // examine message, do smth with it, reply with next callback
      return next(null, 'delivered!');
  }
}

AMQPTransport.connect(configuration, router).then(function (amqp) {
  // amqp instance
  // you can use it do directly send messages
  // amqp.publish
  // amqp.publishAndWait
  // amqp.send
  // amqp.sendAndWait
  //
  // This all internally uses [amqp-coffee publish method](https://github.com/dropbox/amqp-coffee#connectionpublishexchange-routingkey-data-publishoptions-callback)
  // If message fails - it will be rejected in a promise
});
```

### amqp.publish(route, message, [options])

Publishes message to a route on an exchange defined in configuration

* `route` - routingKey, must be string
* `message` - anything that can be stringified, it's advised for it to be small
* `options`:
  * `options.timeout` - sets TTL on the message

### amqp.publishAndWait(route, message, [options])

Same as previous one, but specifies a correlation id and replyTo header, therefore allowing one
to receive a response.

* `route` - routikngKey, must be string
* `message` - anything that can be stringified, it's advised for it to be small
* `options`:
  * `options.timeout` - sets TTL on the message
  * `options.priority` - 0 to 255 - priority of the message if queue was declared with `x-max-priority`

### amqp.send(queue, message, [options])

Sends message directly to a queue

* `queue` - queue name, must be string
* `message` - anything that can be stringified, it's advised for it to be small
* `options`:
  * `options.timeout` - sets TTL on the message

### amqp.sendAndWait(queue, message, [options])

Sends message directly to a queue and sets replyTo and correlationId headers

* `queue` - queue name, must be string
* `message` - anything that can be stringified, it's advised for it to be small
* `options`:
  * `options.timeout` - sets TTL on the message

## Tests

Run `make test` in order to run tests in the dockerized infrastructure. Currently runs tests in 5.x.x, 4.x.x and 0.10.40.
Add more targets if you wish to.
