import * as z from 'zod'

const kLocalhost = 'localhost'

export const Host = z.union([
  z.string().transform((it) => {
    if (it === '') {
      return kLocalhost
    }
    return it
  }),
  z.string().array().min(1),
  z.object({
    host: z.string().nonempty(),
    port: z.number().positive(),
  })
    .array()
    .min(1)
])
  .default('localhost')

export const ConnectionOpts = z.object({
  /** rabbitmq host */
  host: Host,

  /** rabbitmq port */
  port: z.number()
    .default(5672),

  /** heartbeat check */
  heartbeat: z.number()
    .default(10000),

  /** rabbitmq login */
  login: z.string()
    .default('guest'),

  /** rabbitmq password */
  password: z.string()
    .default('guest'),

  /** rabbitmq virtual host */
  vhost: z.string()
    .default('/'),

  /** temporary channel close time with no activity */
  temporaryChannelTimeout: z.number()
    .default(6000),

  /** enable auto-reconnect */
  reconnect: z.boolean()
    .default(true),

  /** reconnect delay time */
  reconnectDelayTime: z.number()
    .default(500),

  /** select host to connect to randomly */
  hostRandom: z.boolean()
    .default(false),

  /** whether to use SSL */
  ssl: z.boolean()
    .default(false),

  /** TODO: ssl options */
  sslOptions: z.any(),

  /** disable Nagle\'s algorithm */
  noDelay: z.boolean()
    .default(true),

  /** options for advertising client properties */
  clientProperties: z.object({
    capabilities: z.object({
      /** whether to react to cancel events */
      consumer_cancel_notify: z.boolean()
        .default(true),
    })
      // @ts-expect-error
      .default({})
  })
    // @ts-expect-error
    .default({})
})

export type Host = z.infer<typeof Host>
export type ConnectionOpts = z.infer<typeof ConnectionOpts>
