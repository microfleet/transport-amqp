# RabbitMQ / AMQP Node.js Transport for Microservices

<img alt="Microfleet AMQP" src="https://raw.githubusercontent.com/microfleet/transport-amqp/master/assets/mf-concept-amqp.png" width="412" height="208" />

Contains RabbitMQ-based transport for establishing a net of loosely coupled microservices with a simple RPC-style calling interface using Node.js

[![npm version](https://badge.fury.io/js/%40microfleet%2Ftransport-amqp.svg)](https://badge.fury.io/js/%40microfleet%2Ftransport-amqp)
[![Build Status](https://semaphoreci.com/api/v1/makeomatic/transport-amqp/branches/master/shields_badge.svg)](https://semaphoreci.com/makeomatic/transport-amqp)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg?style=flat-square)](https://github.com/semantic-release/semantic-release)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fmicrofleet%2Ftransport-amqp.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fmicrofleet%2Ftransport-amqp?ref=badge_shield)

## Install

`npm i @microfleet/transport-amqp -S`

`yarn add @microfleet/transport-amqp`

Heavily relies on `dropbox/amqp-coffee` & `@microfleet/amqp-coffee` lib for establishing communication with RabbitMQ using AMQP protocol

## Usage

Using AMQP is often not as easy as it sounds - managing connection to your cluster, lifecycle of the queues, bindings & exchanges, as well guaranteed delivery of messages - that all can be a burden. This module has been used in production for over 5 years now and had many iterations to fine-tune reconnection strategies and message delivery issues

Essentially it gives you an opinionated subset of AMQP spec to create guaranteed completion worker queues or an RPC service.

### Configuration

Please consult with [configuration schemas](src/schema.js) where every possible setting is annotated and described

### Establish connection

There are several strategies that are supported with this module out-of-the-box:

* 1 consumption queue for all routes (internally - 1 channel that consumes a queue bound to all routing keys)
* 1 RPC queue (internally uses 1 channel for publishing and 1 channel for consumption of responses)
* 1 consumption queue per each route

And mix of each of these strategies.

#### Long-running microservice

##### Message handling

Each message that is delivered through AMQP to consumer will be received using the following routing function.
Messages must be encoded in JSON, which is transparently handled by this library and follow idiomatic { err, message } structure for Node.js

```js
const router = (message, properties, actions, next) => {
  // message - anything that was sent to the consumer
  // if sent using this library - you can send any type of data supported by Node.js
  // except for streams

  // properties - this is basic message content
  // more info can be found here - https://www.rabbitmq.com/amqp-0-9-1-reference.html#class.basic
  // those of value are:
  // `properties.headers`, `properties.correlationId`, `properties.replyTo`

  // actions - if `neck` (prefetchCount >= 0, noAck: true) is defined, it would have
  //  - .ack()
  //  - .reject()
  //  - .retry()

  // next - standard callback with (err, response), if no `replyTo` is set response will only be logged into
  // console. This would be the case when someone published a message and they don't care about the response
  // typically that would happen when the task is considered long-running and we can't reliably respond fast
  // enough for the publisher
};
```

##### One permanent queue - many bound routes

```js
const AMQPTransport = require('@microfleet/transport-amqp');
const options = {
  // will create queue with the following name:
  queue: 'permanent-queue-name',
  // will bind that queue on the exchange
  listen: ['routing.key', 'prefix.#', '*.log'],
  // will create exchange with that name
  exchange: 'node-services',
};

AMQPTransport.connect(options, router).then((amqp) => {
  // at this point we've connected
  // created a consumed queue and router is called when messages are delivered to it
  // amqp is a connected instance of an AMQPTransport
});
```

##### Permanent queue per bound route

```js
const AMQPTransport = require('@microfleet/transport-amqp');
const options = {
  // will create queue with the following name:
  queue: 'permanent-queue-name',
  // will bind that queue on the exchange
  listen: ['routing.key', 'prefix.#', '*.log'],
  // will create exchange with that name
  exchange: 'node-services',
};

// this is an Array of queue options for each queue
// you can overwrite queue names here
const queueOpts = [{
  // extra settings for queue `permanent-queue-name-routing.key`
  arguments: {
    'x-dead-letter-exchange': 'something'
  }
}, {
  // overwrite name of the queue for the second route
  queue: 'awesome-queue',
}];

AMQPTransport.multiConnect(options, router, queueOpts).then((amqp) => {
  // at this point we've connected
  // created several consumed queues with names:
  //  * `permanent-queue-name-routing.key` bound to `routing.key`
  //  * `awesome-queue` bound to `prefix.#`
  //  * `permanent-queue-name-..log` bound to `*.log`
  // amqp is a connected instance of an AMQPTransport
});
```

#### RPC client

Once you have a long-running microservice handling messages - you can interact with it using same adapter

```js
const AMQPTransport = require('@microfleet/transport-amqp');
const opts = { private: true }; // establish private queue right after connecting

// no router passed - means we are not creating consumed queue right away
// we may still do it later, but for now it's only good to do RPC calls
AMQPTransport.connect(opts).then((amqp) => {
  // we've connected to RabbitMQ server and are ready to send messages
  // send messages using routing keys:

  // return Promise, which resolves
  // based on publishOptions
  //   * confirm - waits for commit from AMQP server before resolving
  //   * immediate - waits for the message to be delivered, if it can't be - rejects
  //   * other options - read more in the schema.js file linked earlier
  amqp.publish(routingKey, message, publishOptions, [parentSpan])
    .then(() => {
      // sent
    })
    .catch((err) => {
      // failed to send
    })

  // same as publish, but sets correlation-id and reply-to properties
  // on the message, allowing consumer to response
  // resolves with
  amqp.publishAndWait(routingKey, message, publishOptions, [parentSpan])
    .then((response) => {
      // do whatever you want
    })
    .catch((err) => {
      // either failed to send or response contained an error - work with it here
    })

  // Other option is to work not with the routing keys, but with queues directly
  // for that there are 2 similar methods
  // apart from `routingKey` and `queueName` - everything else works the same way
  amqp.send(queueName, message, publishOptions, [parentSpan])
  amqp.sendAndWait(queueName, message, publishOptions, [parentSpan])
});
```

#### Graceful shutdown

If the graceful shutdown of your service is needed, to stop receiving incoming messages but continue processing, call `closeAllConsumers()`.

This method closes all consumers but leaves the transport connection active. You can process all incoming messages and securely close connections.

```js
AMQPTransport.connect(options, router).then((amqp) => {
  service.on('close', async () => {
    await amqp.closeAllConsumers();
    // do everything you need
    // ..
    await amqp.close();
  })
});
```

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fmicrofleet%2Ftransport-amqp.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fmicrofleet%2Ftransport-amqp?ref=badge_large)
