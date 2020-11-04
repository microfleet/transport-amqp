import uniq from 'lodash/uniq'
import { getInstance as getLoggerInstance } from '../loggers'
import { SequenceProvider } from '../sequence-provider'
import { WellKnowHeaders } from '../types'
import { on406 } from '../utils/error'
import { AMQP } from '../utils/transport'

const kPrivateToken = Symbol.for('queue-private-ctr-token')

export interface CreateQueueProps {
  options: AMQP.QueueOpts
}

export class Queue {
  public name: string

  #routes: string[] = []
  readonly #queue: AMQP.Queue
  readonly #options: AMQP.QueueOpts

  static async create(builder: AMQP.Instance['queueAsync'], opts: AMQP.QueueOpts){
    const queue = await builder(opts)

    try {
      await queue.declareAsync()
    } catch (err) {
      if (err.replyCode === 406) {
        on406.call({ log: getLoggerInstance() }, opts, err)
      } else {
        throw err
      }
    }

    return new Queue(kPrivateToken, queue, {
      options: {
        ...queue.queueOptions
      },
    })
  }

  static sequence = new SequenceProvider({ urlSafe: true })
  static getName(replyTo?: string) {
    return replyTo ?? `microfleet.${Queue.sequence.next()}`
  }

  constructor(token: symbol, queue: AMQP.Queue, props: CreateQueueProps) {
    if (token !== kPrivateToken) {
      throw new Error('Calling private constructor')
    }

    this.name = props.options.queue
    this.#queue = queue
    this.#options = props.options
  }

  get options() {
    return this.#options
  }

  get log() {
    return getLoggerInstance()
  }

  get routes() {
    return this.#routes
  }

  set routes($routes: string[]) {
    this.#routes = $routes.length !== 0
      ? uniq($routes)
      : $routes
  }

  /**
   * Binds exchange to queue via route. For Headers exchange
   * automatically populates arguments with routing-key: <route>.
   * @param  {string} exchange - Exchange to bind to.
   * @param  {string} route - Routing key.
   * @param  {boolean} [headerName=false] - if exchange has `headers` type.
   * @returns {Promise<*>}
   */
  async bindRoute(exchange: string, route: string, headerName: boolean | string = false) {
    let routingKey = ''
    const queueName = this.name
    const options = {} as AMQP.QueueOpts

    if (headerName === false) {
      routingKey = route
    } else {
      options.arguments = {
        [WellKnowHeaders.XMatch]: 'any',
        [headerName === true ? WellKnowHeaders.RoutingKey : headerName]: route,
      }
    }

    const response = await this.#queue
      .bindAsync(exchange, routingKey, options)
    const { _routes: routes } = this.#queue

    if (Array.isArray(routes)) {
      // reconnect might push an extra route
      if (!routes.includes(route)) {
        routes.push(route)
      }

      this.log.trace({ routes, queueName }, '[queue routes]')
    }

    this.log.debug({ queueName, exchange, routingKey }, 'bound queue to exchange')
    return response
  }
}

