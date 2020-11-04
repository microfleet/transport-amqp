import EventEmitter from 'eventemitter3'
import { RawMessage } from '../types'
import { QueueConfOpts } from '../queue'

const kPromisified = Symbol.for('@microfleet/amqp-promisified');

// Promisify stuff
['Exchange', 'Queue', 'Connection', 'Consumer', 'Publisher'].forEach((name) => {
  const path = require.resolve(`@microfleet/amqp-coffee/bin/src/lib/${name}`)
  const mod = require(path) // eslint-disable-line import/no-dynamic-require

  if (mod[kPromisified]) return

  // @ts-expect-error
  Promise.promisifyAll(mod.prototype)
  mod[kPromisified] = true
});

const amqp = require('@microfleet/amqp-coffee')

amqp.prototype.consumeAsync = async function consumeAsync(...args: any[]) {
  let consumer

  // @ts-expect-error
  await Promise.fromCallback((next) => {
    consumer = this.consume(...args, next)
  });

  return consumer
}

export namespace AMQP {
  export interface Instance extends EventEmitter {
    new(config: any, next: (err: any, value?: any) => void): Instance

    [k: string]: any
    state: 'opening' | 'open' | 'reconnecting'
    serverProperties: ServerProperties

    queueAsync(opts: QueueOpts): Promise<Queue>
    consumeAsync<
      RawBody extends any = any
    >(
      queue: string,
      opts: ConsumerOpts,
      handler: IncomingMessageHandler<RawBody>
    ): Promise<Consumer>
    exchangeAsync(opts: ExchangeOpts): Promise<Exchange>
  }

  export interface IncomingMessageHandler<RawBody extends any> {
    (incoming: RawMessage<RawBody>): void
  }

  export interface ServerProperties {
    cluster_name: string
    version: string
    [k: string]: any
  }

  export interface Queue {
    _routes: string[]
    queueOptions: QueueOpts

    bindAsync(exchange: string, routingKey: string, options?: QueueOpts): Promise<void>
    unbindAsync(exchange: string, route: string): Promise<void>

    [k: string]: any
  }

  export interface QueueOpts extends QueueConfOpts {
    queue: string
    arguments: QueueConfOpts['arguments'] & {
      'x-match'?: string
    }
  }

  export interface Consumer extends EventEmitter {
    [k: string]: any
  }

  export interface ConsumerOpts extends QueueOpts {
    noAck: boolean
    prefetchCount?: number
    [k: string]: any
  }

  export interface ConsumerError extends Error {
    replyCode:
      | 311
      | 313
      // access-refused  403
      //  The client attempted to work with a server entity
      //  to which it has no access due to security settings.
      | 403
      // not-found  404
      //  The client attempted to work with a server entity that does not exist.
      | 404
      // resource-locked  405
      //  The client attempted to work with a server entity
      //  to which it has no access because another client is working with it.
      | 405
      // precondition-failed  406
      //  The client requested a method that was not allowed
      //  because some precondition failed.
      | 406
    replyText: string
  }

  export interface Exchange {
    declareAsync(opts?: ExchangeOpts): Promise<Exchange>
    deleteAsync(opts?: ExchangeDeleteOpts): Promise<void>
    bindAsync(dstExc: string, routingKey: string, srcExc?: string): Promise<void>
    unbindAsync(dstExc: string, routingKey: string, srcExc?: string): Promise<void>
    [k: string]: any
  }

  export enum ExchangeType {
    Topic = 'topic',
    Direct = 'direct',
    Fanout = 'fanout',
    Headers = 'headers',
  }

  export interface ExchangeOpts {
    exchange: string
    type?: ExchangeType
    durable?: boolean
    autoDelete?: boolean
    noWait?: boolean
    internal?: boolean
    passive?: boolean
  }

  export type ExchangeDeleteOpts = Pick<ExchangeOpts, 'exchange' | 'noWait'> & {
    ifUnused?: boolean
  }
}

export default amqp as AMQP.Instance
