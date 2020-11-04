import * as z from 'zod'
import { ContentEncoding, ContentType } from '../types'

export enum DeliveryMode {
  Transient = 1,
  SaveOnDisc = 2,
}

export const DeliveryModeEnum = z.nativeEnum(DeliveryMode)
  .default(DeliveryMode.Transient)

export const PublishingHeaders = z.record(z.union([
  z.number(),
  z.string(),
  z.undefined()
]))

export const PublishingConfOpts = z.object({
  /** 1 - transient, 2 - saved on disk */
  deliveryMode: DeliveryModeEnum,

  /** whether to wait for commit confirmation */
  confirm: z.boolean()
    .default(false),

  /** when true and message cant be routed to a queue - exception returned, otherwise its dropped */
  mandatory: z.boolean()
    .default(false),

  /** not implemented by rabbitmq */
  immediate: z.boolean()
    .default(false),

  /** TODO: enum? */
  /** default content-type for messages */
  contentType: z.string()
    .default(ContentType.Json),

  /** default content-encoding */
  contentEncoding: z.string()
    .default(ContentEncoding.Plain),

  /** additional headers */
  headers: PublishingHeaders
    .default({}),

  /** whether to return only response data or include headers etc. */
  simpleResponse: z.boolean()
    .default(true),
})

export type PublishingConfOpts = z.infer<typeof PublishingConfOpts>
export type PublishingConfHeaders = z.infer<typeof PublishingHeaders>
