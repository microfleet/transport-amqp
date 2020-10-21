import noop from 'lodash/noop'
import pTimeout from 'p-timeout'

import { AMQP } from '../utils/transport'
import { Queue } from '../queue'
import { ConsumerFactory } from './factory'
import { AnyFunction, MessageHandler, MessagePreHandler } from '../types'

import { getInstance as getLoggerInstance } from '../loggers'

type AnyConsumerError = AMQP.ConsumerError | {
  error: AMQP.ConsumerError
}

export interface OnConsumerError {
  (consumer: Consumer, shouldRecreate: boolean, err: AMQP.ConsumerError, res?: any): void
}

export interface OnConsumerCancel {
  (consumer: Consumer, err: AMQP.ConsumerError, res?: any): void
}

export interface OnConsumerClose {
  (consumer: Consumer): void
}

export interface ConsumerOpts {
  neck?: number
  queue: Queue
  onMessage: MessageHandler
  onMessagePre?: MessagePreHandler

  onError?: OnConsumerError
  onCancel?: OnConsumerCancel
  onClose?: AnyFunction
}

const kPrivateToken = Symbol.for('consumer-private-ctr-token')

export class Consumer {
  #initialized: boolean = false
  readonly #consumer: AMQP.Consumer

  protected readonly onErrorCallback: OnConsumerError
  protected readonly onCloseCallback: OnConsumerClose
  protected readonly onCancelCallback: OnConsumerCancel
  protected readonly boundQueue: Queue

  static async create<T extends typeof Consumer>(this: T, builder: ConsumerFactory['buildConsumer'], opts: ConsumerOpts): Promise<InstanceType<T>> {
    const qos = Consumer.getQoS(opts)
    const consumer = await builder(
    opts.queue.name,
    qos, {
      onMessage: opts.onMessage,
      onMessagePre: opts.onMessagePre,
    })

    return (new this(kPrivateToken, consumer, opts)) as InstanceType<T>
  }

  static getQoS = (opts: ConsumerOpts): AMQP.ConsumerOpts => {
    const { queue, neck } = opts
    const consumeOpts = {
      ...queue.options,
    } as AMQP.ConsumerOpts

    if (neck === undefined) {
      consumeOpts.noAck = true
    } else {
      consumeOpts.noAck = false
      consumeOpts.prefetchCount = neck > 0 ? neck : 0
    }

    return consumeOpts
  }

  constructor(token: symbol, consumer: AMQP.Consumer, opts: ConsumerOpts) {
    if (token !== kPrivateToken) {
      throw new Error('Calling private constructor')
    }

    this.#consumer = consumer
    this.boundQueue = opts.queue
    this.onErrorCallback = opts.onError ?? noop
    this.onCancelCallback = opts.onCancel ?? noop
    this.onCloseCallback = opts.onClose ?? noop

    this.initListeners()
  }

  get log() {
    return getLoggerInstance()
  }

  get consumer() {
    return this.#consumer
  }

  get consumerTag() {
    return this.#consumer.consumerTag
  }

  async close() {
    this.#consumer.removeAllListeners()
    this.#consumer.on('error', noop)
    this.onCloseCallback(this)

    await this.#cancel()
  }

  protected initListeners = () => {
    const consumer = this.consumer

    // remove previous listeners if we re-use the channel
    // for any reason
    if (this.#initialized) {
      consumer.removeAllListeners('error')
      consumer.removeAllListeners('cancel')
    }

    consumer.on('error', this.handleError)
    consumer.on('cancel', this.handleCancel)

    this.#initialized = true
  }

  protected handleError = (err: AnyConsumerError, res?: any) => {
    return 'error' in err
      ? this.onError(err.error, res)
      : this.onError(err, res)
  }

  protected handleCancel = (err: AMQP.ConsumerError, res?: any) => {
    return this.onCancelCallback(this, err, res)
  }

  protected onError = (err: AMQP.ConsumerError, res?: any) => {
    let shouldRecreate = false

    switch (err.replyCode) {
      // ignore errors
      case 311:
      case 313:
        this.log.error({ err, res }, 'error working with a channel:')
        break

      case 404:
        if (err.replyText && err.replyText.includes(this.boundQueue.name)) {
          shouldRecreate = true
        }
        break

      default:
        this.log.warn({ err }, 'unhandled consumer error')
        shouldRecreate = true
        break
    }

    this.onErrorCallback(this, shouldRecreate, err, res)
  }

  #cancel = async () => {
    const cancellation = new Promise((resolve, reject) => {
      this.consumer.cancel((err: any, res: any) => {
        if (err) {
          return reject(err)
        }
        resolve(res)
      })
    })

    try {
      return pTimeout(cancellation, 5000)
    } catch (err) {
      if (!(err instanceof pTimeout.TimeoutError)) {
        throw err
      }
    } finally {
      this.log.info({ consumerTag: this.consumer.consumerTag }, 'closed consumer')
    }
  }
}

