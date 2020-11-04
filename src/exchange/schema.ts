import * as z from 'zod'

export enum ExchangeType {
  Topic = 'topic',
  Direct = 'direct',
  Fanout = 'fanout',
  Headers = 'headers',
}

export const ExchangeTypeEnum = z.nativeEnum(ExchangeType)

export const ExchangeConfOpts = z.object({
  /** do not autoDelete exchanges */
  autoDelete: z.boolean()
    .default(false),

  /** whether not to wait for declare response */
  noWait: z.boolean()
    .default(false),

  /** whether to set internal bit */
   internal: z.boolean()
    .default(false),

  /** type of the exchange */
  type: ExchangeTypeEnum
    .default(ExchangeType.Topic),

  /** whether to preserve exchange on rabbitmq restart */
  durable: z.boolean()
    .default(true),
})

export type ExchangeConfOpts = z.infer<typeof ExchangeConfOpts>
