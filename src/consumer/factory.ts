import pick from 'lodash/fp/pick'

import { AMQP } from '../utils/transport'
import { parseInput } from '../utils/parsing'
import { EntityStore } from '../entity-store'
import { getInstance as getLoggerInstance } from '../loggers'

import { Consumer, ConsumerOpts } from './consumer'
import { PrivateConsumer } from './private-consumer'
import { MessageHandler, MessagePreHandler, RawMessage } from '../types'

export interface ConsumerFactoryOpts {
  amqp: AMQP.Instance
}

export interface Creator<T, B, O extends any> {
  create(builder: B, opts: O): Promise<T>
}

export type Consumers =
  | Consumer
  | PrivateConsumer

export type ConsumerCreator<T> = Creator<
  T,
  ConsumerFactory['buildConsumer'],
  ConsumerOpts
>

export class ConsumerFactory extends EntityStore<Consumer> {
  #amqp: AMQP.Instance
  readonly #parseInput: typeof parseInput

  static pickExtendedMessageProps = pick([
    'deliveryTag',
    'redelivered',
    'exchange',
    'routingKey',
    'weight',
  ])

  constructor(opts: ConsumerFactoryOpts) {
    super(Map)
    this.#amqp = opts.amqp
    this.#parseInput = parseInput.bind(this)
  }

  get log() { return getLoggerInstance() }

  buildConsumer = (queue: string, opts: AMQP.ConsumerOpts, handlers: {
    onMessage: MessageHandler<any, any>,
    onMessagePre?: MessagePreHandler<any>
  }) => {
    const onMessage = this.#createMessageHandler(
      handlers.onMessage,
      handlers.onMessagePre
    )

    return this.#amqp.consumeAsync(queue, opts, onMessage)
  }

  // create(opts: ConsumerOpts) {
  //   return this.create(Consumer, opts)
  // }
  //
  // createPrivate(opts: ConsumerOpts) {
  //   // TODO
  //   // https://github.com/microsoft/TypeScript/issues/5863
  //   return this.create(PrivateConsumer, opts) as Promise<PrivateConsumer>
  // }

  async create<T extends Consumer>(Type: ConsumerCreator<T>, opts: ConsumerOpts): Promise<T> {
    this.log.info({ queue: opts.queue.name }, 'consumer is being created')
    const consumer = await Type
      .create(this.buildConsumer, opts)
    this.log.info({ queue: opts.queue.name }, 'consumer is created')

    return consumer
  }

  async close (consumer: Consumer){
    await consumer.close()
  }

  #createMessageHandler = <
    RequestBody extends any = any,
    ResponseBody extends any = any
  >(
    onMessage: MessageHandler<RequestBody, ResponseBody>,
    onMessagePre?: MessagePreHandler<RequestBody>
  ) => (
    async (incoming: RawMessage<RequestBody>) => {
      if (onMessagePre) {
        onMessagePre(incoming)
      }

      // extract message data
      const { properties } = incoming;
      const { contentType, contentEncoding } = properties

      // parsed input data
      const message = await this.#parseInput(
        incoming.raw,
        contentType,
        contentEncoding
      )

      // useful message properties
      const props = {
        ...properties,
        ...ConsumerFactory.pickExtendedMessageProps(incoming),
      }

      // pass to the consumer message router
      // message - properties - incoming
      //  incoming.raw<{ ack: ?Function, reject: ?Function, retry: ?Function }>
      //  and everything else from amqp-coffee
      setImmediate(onMessage, message, props, incoming)
    }
  )
}
