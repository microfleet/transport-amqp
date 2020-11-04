import * as z from 'zod'
import { ExchangeType, ExchangeTypeEnum } from '../exchange'

export const DeadLetterExchangeOpts = z.object({
  /** enabled DLX by default for fast-reply when messages are dropped */
  enabled: z.boolean()
    .default(true),

  params: z
    .object({
      /** dead letters are redirected here */
      exchange: z.string()
        .default('amq.headers'),

      /** must be headers for proper built-in matching */
      type: ExchangeTypeEnum
        .default(ExchangeType.Headers),

      /** DLX persistance */
      autoDelete: z.boolean()
        .default(false),
    })
    // @ts-expect-error
    .default({})
})

export type DeadLetterExchangeOpts = z.infer<typeof DeadLetterExchangeOpts>
