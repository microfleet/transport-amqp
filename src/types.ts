import { Consumer } from './consumer'
import { Queue } from './queue'
import { PublishingConfOpts } from './schema/publishing'
import { AMQP } from './utils/transport'

export type AnyFunction = (...args: any[]) => any

export interface PublishingOpts extends PublishingConfOpts {
  appId: string
  replyTo: string
  correlationId: string
  timeout?: number
  exchange?: string
  arguments?: Record<string, any>
}

export interface ExtendedMessage {
  weight: number
  exchange: string
  routingKey: string
  deliveryTag: string
  redelivered: boolean
}

export interface MessageHeaders extends Record<string | number, string | number> {
  contentType: ContentType
  contentEncoding: ContentEncoding
}

export interface RawMessage<
  ResponseBody extends any
> extends ExtendedMessage {
  raw: Buffer
  ack: AnyFunction
  data: ResponseBody
  size: number
  retry: AnyFunction
  reject: AnyFunction
  properties: MessageHeaders
  [x: string]: any
}

export type MessageProps = MessageHeaders & ExtendedMessage

export interface MessageHandler<
  RequestBody extends any = any,
  ResponseBody extends any = any
> {
  (payload: RequestBody, properties: PublishingOpts, raw: RawMessage<RequestBody>): PromiseLike<ResponseBody>
}

export interface MessagePreHandler<RequestBody extends any = any> {
  (raw: RawMessage<RequestBody>): void
}

export interface ConsumedQueue {
  queue: Queue
  options: AMQP.QueueOpts
  consumer?: Consumer
}

export const enum ContentType {
  Utf8 = 'string/utf8',
  Json = 'application/json',
}

export const enum ContentEncoding {
  Gzip  = 'gzip',
  Plain = 'plain',
}

export const enum AMQPTransportEvents {
  // pre-processing hook
  Pre               = 'pre',
  Ready             = 'ready',
  Close             = 'close',
  Error             = 'error',
  ConsumerClose     = 'consumer-close',
  PrivateQueueReady = 'private-queue-ready',
  ConsumedQueueReconnected = 'consumed-queue-reconnected',
}
