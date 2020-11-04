import opentracing from 'opentracing'
import { Consumer } from './consumer'
import { MessageHeaders, PublishOptions } from './message-options'

import { Queue } from './queue'
import { AMQP } from './utils/transport'

export type AnyFunction = (...args: any[]) => any

export const enum WellKnowHeaders {
  ReplyTo = 'reply-to',
  Timeout = 'timeout',
  XDeath  = 'x-death',
  XMatch  = 'x-match',
  RoutingKey = 'routing-key',
  ContentType = 'contentType',
  ContentEncoding = 'contentEncoding',
}

export interface ExtendedMessage {
  replyTo?: string
  correlationId?: string
  weight: number
  exchange: string
  routingKey: string
  deliveryTag: string
  redelivered: boolean
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

export interface MessageHandler<
  RequestBody extends any = any,
  ResponseBody extends any = any
> {
  (
    payload: RequestBody,
    properties: PublishOptions,
    raw: RawMessage<RequestBody>,
    responseHandler: (error?: Error, data?: ResponseBody) => void
  ): PromiseLike<ResponseBody>
}

export interface MessagePreHandler<RequestBody extends any = any> {
  (raw: RawMessage<RequestBody>): void
}

export interface PublishMessageHandle<
  RequestBody extends any = any,
  ResponseBody extends any = any
> {
  (
    route: string,
    message: RequestBody,
    options: Partial<PublishOptions>,
    parentSpan?: opentracing.Span | opentracing.SpanContext
  ): Promise<ResponseBody>
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
  Pre               = 'pre',
  After             = 'after',
  Ready             = 'ready',
  Close             = 'close',
  Error             = 'error',
  Publish           = 'publish',
  ConsumerClose     = 'consumer-close',
  PrivateQueueReady = 'private-queue-ready',
  ConsumedQueueReconnected = 'consumed-queue-reconnected',
}
