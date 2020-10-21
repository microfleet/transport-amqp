import * as z from 'zod'

export const QueueConfOpts = z.object({
  /** TODO: description */
  autoDelete: z.boolean()
    .optional(),

  /** TODO: description */
  exclusive: z.boolean()
    .optional(),

  /** TODO: description */
  noWait: z.boolean()
    .optional(),

  /** TODO: description */
  passive: z.boolean()
    .optional(),

  /** survive restarts & use disk storage */
  durable: z.boolean()
    .default(true),

  arguments: z.object({
    /** delete queue after it's been unused for X seconds */
    'x-expires': z.number()
      .min(0)
      .default(1800000)
      .optional(),

    /** setup priority queues where messages will be delivery based on priority level */
    'x-max-priority': z.number()
      .min(2)
      .max(255)
      .optional(),
  })
    .default({})
})
  // @ts-expect-error
  .default({})

export type QueueConfOpts = z.infer<typeof QueueConfOpts>
