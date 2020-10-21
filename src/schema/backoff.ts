import * as z from 'zod'

export enum BackoffPolicy {
  Private = 'private',
  Consumed = 'consumed',
}

export const BackoffPolicyEnum = z.nativeEnum(BackoffPolicy)

export const BackoffPolicyOpts = z.object({
  /** min delay for attempt #1 */
  min: z.number()
    .min(0)
    .default(250),

  /** max delay */
  max: z.number()
    .min(0)
    .default(1000),

  /** exponential increase factor */
  factor: z.number()
    .min(1)
    .default(1.2)
})
  .refine((val) => (
    val.max > val.min
  ), {
    message: 'min must be less than or equal to max'
  })

export const BackoffOpts = z.object({
  [BackoffPolicy.Private]: BackoffPolicyOpts
    // @ts-expect-error
    .default({}),

  [BackoffPolicy.Consumed]: BackoffPolicyOpts
    .default({
      min: 500,
      max: 5000,
      factor: 1.2,
    }),
})

export type BackoffOpts = z.infer<typeof BackoffOpts>
export type BackoffPolicyOpts = z.infer<typeof BackoffPolicyOpts>
