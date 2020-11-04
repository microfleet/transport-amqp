import * as z from 'zod'

import { QueueConfOpts } from '../queue'
import { ConnectionOpts } from './connection'
import { PublishingConfOpts } from './publishing'
import { DeadLetterExchangeOpts } from './dlx'
import { CoercedUniqStringArray } from './helpers'
import { ExchangeConfOpts, ExchangeType } from '../exchange'
import { LoggerLike } from './logger-like'
import { BackoffOpts } from './backoff'

export const Schema = z.object({
  /** Name of the service when advertising to AMQP */
  name: z.string()
    .default('amqp'),

  /** Optional logger instance */
  log: LoggerLike
    .optional(),

  /** when true - initializes private queue right away */
  private: z.boolean()
    .default(false),

  /** size of LRU cache for responses, 0 to disable it */
  cache: z.number()
    .min(0)
    .default(100),

  /** default *AndWait timeout */
  timeout: z.number()
    .default(10000),

  /** enables debug messages */
  debug: z.boolean()
    .default(process.env.NODE_ENV !== 'production'),

  /** attach default queue to these routes on default exchange */
  listen: CoercedUniqStringArray,

  /** advertise end-client service version */
  version: z.string()
    .default('n/a'),

  /** if defined - queues will enter QoS mode with required ack & prefetch size of neck */
  neck: z.number()
    .min(0),

  /** TODO: set correct type */
  tracer: z.any(),

  /** options for setting up connection to RabbitMQ */
  connection: ConnectionOpts
    // @ts-expect-error
    .default({}),

  /** recovery settings */
  recovery: BackoffOpts
    // @ts-expect-error
    .default({}),

  /** default exchange for communication */
  exchange: z.string()
    .nonempty()
    .default('node-services'),
  exchangeArgs: ExchangeConfOpts
    // @ts-expect-error
    .default({}),

  /** whether to bind queues created by .createConsumedQueue to headersExchange */
  bindPersistentQueueToHeadersExchange: z.boolean()
    .default(false),

  /** this exchange is used to support delayed retry with QoS exchanges */
  headersExchange: ExchangeConfOpts.extend({
    /** default headers exchange to use, should be different from DLX headers exchange */
    exchange: z.string()
      .default('amq.match'),

    /** type of the exchange */
    type: z.literal(ExchangeType.Headers)
      .default(ExchangeType.Headers)
  })
    // @ts-expect-error
    .default({}),

  /** default queue to connect to for consumption */
  queue: z.string()
    .nonempty(),

  /** default options for creating consumer queues */
  defaultQueueOpts: QueueConfOpts
    // @ts-expect-error
    .default({}),

  /** default options for private RPC queues */
  privateQueueOpts: QueueConfOpts
    // @ts-expect-error
    .default({}),

  /** default for dead-letter-exchange */
  dlx: DeadLetterExchangeOpts
    // @ts-expect-error
    .default({}),

  /** default options when publishing messages */
  defaultOpts: PublishingConfOpts
    // @ts-expect-error
    .default({}),
})
  .refine(opts => (
    opts.dlx.params.exchange !== opts.headersExchange.exchange
  ), {
    message: 'must use different headers exchanges',
  })
  // @ts-expect-error
  .default({})

export type Schema = z.infer<typeof Schema>
