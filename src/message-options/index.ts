import { kReplyHeaders } from '../constants'
import { PublishingConfHeaders, PublishingConfOpts } from '../schema/publishing'
import { ContentEncoding, ContentType, WellKnowHeaders } from '../types'

export interface MessageHeaders
  extends PublishingConfHeaders,
    Record<WellKnowHeaders, string | number | undefined>
{
  contentType: ContentType
  contentEncoding: ContentEncoding
}

// TODO
// WTF? message headers are exactly the same according to the old code
export interface PublishOptions extends PublishingConfOpts {
  appId: string
  replyTo?: string
  correlationId?: string
  routingKey: string
  headers: Partial<MessageHeaders>
  gzip?: boolean
  cache?: number
  timeout?: number
  exchange?: string
  arguments?: Record<string, any>
  expiration?: string
  skipSerialize?: boolean
  contentType: ContentType
  contentEncoding: ContentEncoding
  [kReplyHeaders]: Partial<MessageHeaders>
}

export interface ReplyOptions {
  simpleResponse?: boolean
}

export class MessageOptions {
  /**
   * Specifies default publishing options
   * @param  {Object} options
   * @param  {String} options.exchange - will be overwritten by exchange thats passed
   *  in the publish/send methods
   *  https://github.com/dropbox/amqp-coffee/blob/6d99cf4c9e312c9e5856897ab33458afbdd214e5/src/lib/Publisher.coffee#L90
   * @return {Object}
   */
  static getPublishOptions = (
    options: Partial<PublishOptions>,
    defaults: Partial<PublishOptions> = {},
    defaultTimeout: number = 30000,
  ) => {
    // remove unused props such as:
    // - skipSerialize
    // - gzip in favor of contentEncoding
    const needsGzip = options.gzip ?? defaults.gzip
    const timeout = options.timeout ?? defaultTimeout
    // explicitly create an object to keep hidden shape
    const publishOpts = {
      appId: options.appId ?? defaults.appId,
      correlationId: options.correlationId ?? defaults.correlationId,
      replyTo: options.replyTo ?? defaults.replyTo,
      routingKey: options.routingKey ?? defaults.routingKey,
      exchange: options.exchange ?? defaults.exchange,

      cache: options.cache ?? defaults.cache,
      confirm: options.confirm ?? defaults.confirm,

      headers: options.headers ?? defaults.headers,
      arguments: options.arguments ?? defaults.arguments,

      deliveryMode: options.deliveryMode ?? defaults.deliveryMode,
      contentType: options.contentType ?? defaults.contentType,
      // enforce contentEncoding
      contentEncoding: needsGzip
        ? ContentEncoding.Gzip
        : options.contentEncoding ?? defaults.contentEncoding,

      immediate: options.immediate ?? defaults.immediate,
      mandatory: options.mandatory ?? defaults.mandatory,
      expiration: options.expiration ?? defaults.expiration,
      simpleResponse: options.simpleResponse ?? defaults.simpleResponse,
    } as PublishOptions

    // append request timeout in headers
    Object.assign(publishOpts.headers, {
      timeout,
    })

    return publishOpts
  }

  static getReplyOptions = (options: ReplyOptions, defaults: Partial<ReplyOptions>) => {
    return {
      simpleResponse: options.simpleResponse ?? defaults.simpleResponse,
    }
  }
}
