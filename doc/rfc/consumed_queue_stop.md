# Consumer Stop support

## Overview and motivation

Currently `amqp-transport` module does not export methods allowing to close consumers independently of transport.

Transport uses WeakMap's for storing consumers and queue references with keys eq `local function` which are not available from outside.

When you need to stop receiving incoming messages and leave connection to rabbitmq alive, you must implement
a wrapper that allows you to save consumer references to be able to cancel consumption on demand.

## Public property `consumers`

Instead of having a WeakMap for established consumers and reconnection handlers we
start retaining them. This allows us to iterate on all the stored consumers and close them
for consuming in an easy fashion. Rest remains the same. One must be careful to cleanup when
they are done with the consumers or a leak might occur

## Public closeAllConsumers() method

Iterates over `consumers` property and calls `stopConsumedQueue` method. Returns Promise with multiple `stopConsumedQueue` Promises.

```js
const transport = AMQPTransport.connect();
// stop only consumers
await transport.closeConsumers();
```
