# Consumer Stop support

## Overview and motivation
Currently `amqp-transport` module does ot export methods allowing to close consumers without closing all transport.
Transport uses WeakMap's for storing consumers and queue references with keys eq `local function`  which are not available from outside.
When you need to stop receiving incoming messages and leave connection alive, You must implement connect and createConsumedQueue wrappers and save references.
These additions will resolve such problems.
## Public property `consumers`
`amqp-transport` instance `consumers` public property, allows access to active consumers in form `Map[consumer]=bindFn`.
 When consumed queue created, reference automatically stored. When Consumer closed, reference deleted. 

For proper handling its contents:
 - added one `consumed-queue-reconnected` event listener `_registerConsumer` method
 - added custom `consumer-close` event listener `_unregisterConsumer` method. Event fired from `helpers.closeConsumer`,
  before `consumer.cancel`. Assuming that event used internally, it fires with `_boundEmit` with `transport` context. 
  In this case, we don't need to wait for any results, because of the event used for keeping `consumers` clean.

## _registerConsumer(consumer, queue, bindFn) method
When consumedQueue created, adds a reference to `consumers`

## _unregisterConsumer(consumer) method
Removes `consumer` reference from `consumers`

## `amqp-transport`.close
For transport proper close, now `close` calls `_close` method.
`_close` method stops all consumers and then closing transport. If an error happened inside
`closeConsumer` method, methods log it and continue the close process

## Public stopConsumers() method
Iterates over `consumers` property and calls `stopConsumedQueue` method. Returns Promise with multiple `stopConsumedQueue` Promises.

```js
const transport = AMQPTransport.connect({});
// stop only consumers 
await transport.closeConsumers();
```
