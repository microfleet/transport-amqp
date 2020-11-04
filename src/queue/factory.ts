import is from '@sindresorhus/is'

import { AMQP } from '../utils/transport'
import { Queue } from './queue'
import { EntityStore } from '../entity-store'
import { QueueConfOpts } from './schema'
import { getInstance as getLoggerInstance } from '../loggers'

export interface QueueOpts {
  queue: string
  options: QueueConfOpts
}

export interface QueueFactoryOpts {
  amqp: AMQP.Instance
}

export class QueueFactory extends EntityStore<Queue> {
  #amqp: AMQP.Instance

  public static ensureQueueOpts = ($opts: QueueOpts | string): QueueOpts => {
    return is.string($opts)
      ? { queue: $opts, options: {} } as QueueOpts
      : { queue: $opts.queue, options: $opts.options }
  }

  public static getQueueOpts = ($opts: QueueOpts | string): AMQP.QueueOpts => {
    const opts = QueueFactory.ensureQueueOpts($opts)
    const { queue } = opts

    return {
      queue,
      ...opts.options,
      durable: opts.options?.durable ?? !!queue,
      autoDelete: opts.options?.autoDelete ?? !queue,
    }
  }

  constructor(opts: QueueFactoryOpts) {
    super()
    this.#amqp = opts.amqp
  }

  get log() {
    return getLoggerInstance()
  }

  buildQueue = async (opts: AMQP.QueueOpts) => (
    this.#amqp.queueAsync(opts)
  )

  async create($opts: QueueOpts | string){
    const opts = QueueFactory.getQueueOpts($opts)

    try {
      const queue = await Queue
        .create(this.buildQueue, opts)
      this.log.info({ queue: queue.name }, 'queue created')
      return queue
    } catch (err) {
      this.log.warn({ err, queue: opts.queue }, 'failed to init queue')
      throw err
    }
  }
}
