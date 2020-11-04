import assert from 'assert'
import { CoercedUniqStringArray } from '../schema/helpers'
import { on406 } from '../utils/error'
import { AMQP } from '../utils/transport'
import { Queue } from '../queue'
import { ExchangeConfOpts, ExchangeType } from './schema'
import { getInstance as getLoggerInstance } from '../loggers'

export interface ExchangeOpts {
  exchange: string
  exchangeArgs: ExchangeConfOpts
}

export type AnyExchangeOpts =
  | ExchangeOpts
  | AMQP.ExchangeOpts

export class ExchangeFactory {
  #amqp: AMQP.Instance

  public static headersExchangeRouteRe = /^[^*#]+$/
  public static isValidHeadersExchangeRoute = (route: string) => {
    return ExchangeFactory.headersExchangeRouteRe.test(route)
  }

  public static getExchangeOpts(opts: AnyExchangeOpts): AMQP.ExchangeOpts {
    assert(opts.exchange, 'exchange name must be specified')
    return 'exchangeArgs' in opts
      ? {
        exchange: opts.exchange,
        ...opts.exchangeArgs,
      }
      : opts
  }

  constructor({ amqp }: { amqp: AMQP.Instance }) {
    this.#amqp = amqp
  }

  get log() { return getLoggerInstance() }

  async create($opts: AnyExchangeOpts): Promise<string> {
    const opts = ExchangeFactory.getExchangeOpts($opts)
    await this.#declareExchange(opts)
    return opts.exchange
  }

  /**
   * Bind specified queue to exchange
   *
   * @param {object}          queue     - queue instance created by .createQueue
   * @param {string|string[]} $routes   - messages sent to this route will be delivered to queue
   * @param {object} [opts={}] - exchange parameters:
   *                 https://github.com/dropbox/amqp-coffee#connectionexchangeexchangeargscallback
   */
  async bindExchange(
    queue: Queue,
    $routes: string | string[],
    opts: AnyExchangeOpts
  ){
    return this.#bindExchange(queue, $routes, opts)
  }

  /**
   * Binds multiple routing keys to headers exchange.
   * @param  {Object}           queue
   * @param  {string|string[]}  $routes
   * @param  {Object}           opts
   * @param  {boolean}          [headerName=false] - if exchange has `headers` type
   * @returns {Promise<*>}
   */
  async bindHeadersExchange(
    queue: Queue,
    $routes: string | string[],
    opts: AnyExchangeOpts,
    headerName: string | boolean = true
  ){
    return this.#bindExchange(
      queue,
      $routes,
      opts,
      headerName
    )
  }

  async bindQueueOnRoutes(
    queue: Queue,
    routes: string[],
    exchanges: {
      regular: AnyExchangeOpts,
      headers?: AnyExchangeOpts,
    },
    oldQueue?: Queue
  ){
    const previousRoutes = oldQueue?.routes ?? [] as string[]

    if (routes.length === 0 && previousRoutes.length === 0) {
      queue.routes = []
      return
    }

    // retrieved some routes
    this.log.debug({ routes, previousRoutes }, 'retrieved routes')
    const rebindRoutes = queue.routes = [...previousRoutes, ...routes]

    const work = [
      this.bindExchange(
        queue,
        rebindRoutes,
        exchanges.regular
      ),
    ]

    // bind same queue to headers exchange
    if (exchanges.headers) {
      work.push(this.bindHeadersExchange(
        queue,
        rebindRoutes,
        exchanges.headers
      ))
    }

    await Promise.all(work)
  }

  #bindExchange = async (
    queue: Queue,
    $routes: string | string[],
    $opts: AnyExchangeOpts,
    headerName: string | boolean = false
  ) => {
    const routes = CoercedUniqStringArray.parse($routes)
    const opts = ExchangeFactory.getExchangeOpts($opts)

    // headers exchange
    // do sanity check
    if (headerName) {
      assert.equal(opts.type, ExchangeType.Headers)
      this.log.debug('bind routes->exchange/headers', routes, opts.exchange)
    } else {
      this.log.debug('bind routes->exchange', routes, opts.exchange)
    }

    const exchange = await this.create(opts)
    const jobs = routes.map((route) => {
      if (headerName) {
        assert.ok(ExchangeFactory.isValidHeadersExchangeRoute(route))
      }

      return queue.bindRoute(exchange, route, headerName)
    })

    return Promise.all(jobs)
  }

  /**
   * Declares exchange and reports 406 error.
   * @param  {Object} params - Exchange params.
   * @returns {Promise<*>}
   */
  #declareExchange = async (opts: AMQP.ExchangeOpts) => {
    try {
      const exchange = await this.#amqp
        .exchangeAsync(opts)
      await exchange.declareAsync()
      return exchange
    } catch (e) {
      if (e.replyCode === 406) {
        return on406.call(this, opts, e)
      }

      throw e
    }
  }
}
