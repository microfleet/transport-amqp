import { Tags, FORMAT_TEXT_MAP } from 'opentracing'

import { AMQPTransport } from '../amqp-transport'
import { safeJSONParse } from './parsing'
import { MessageHandler, PublishingOpts, RawMessage } from '../types'

/**
 * Routing function HOC with reply RPC enhancer
 * @param  {Function} messageHandler
 * @param  {AMQPTransport} transport
 * @returns {Function}
 */
export const initRoutingFn = <
  RequestBody extends any,
  ResponseBody extends any
>(
  messageHandler: MessageHandler<RequestBody, ResponseBody>,
  transport: AMQPTransport
) => {
  /**
   * Response Handler Function. Sends Reply or Noop log.
   * @param  {AMQPMessage} raw - Raw AMQP Message Structure
   * @param  {Error} error - Error if it happened.
   * @param  {mixed} data - Response data.
   * @returns {Promise<*>}
   */
  function responseHandler(this: undefined, raw: RawMessage<RequestBody>, error: Error, data: ResponseBody) {
    const { properties, span } = raw
    return !properties.replyTo || !properties.correlationId
      ? transport.noop(error, data, span, raw)
      : transport.reply(properties, { error, data }, span, raw)
  }

  /**
   * Initiates consumer message handler.
   * @param  {mixed} message - RequestBody passed from the publisher.
   * @param  {Object} properties - AMQP Message properties.
   * @param  {Object} raw - Original AMQP message.
   * @param  {Function} [raw.ack] - Acknowledge if nack is `true`.
   * @param  {Function} [raw.reject] - Reject if nack is `true`.
   * @param  {Function} [raw.retry] - Retry msg if nack is `true`.
   * @returns {Void}
   */
  return function router(
    this: AMQPTransport,
    message: RequestBody,
    properties: PublishingOpts,
    raw: RawMessage<RequestBody>
  ) {
    // add instrumentation
    const appId = safeJSONParse(properties.appId, this.log);

    // opentracing instrumentation
    const childOf = this.tracer.extract(
      FORMAT_TEXT_MAP,
      properties.headers ?? {}
    )
    const span = this.tracer.startSpan(`onConsume:${properties.routingKey}`, {
      childOf,
    });

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_RPC_SERVER,
      [Tags.PEER_SERVICE]: appId.name,
      [Tags.PEER_HOSTNAME]: appId.host,
    })

    // define span in the original message
    // so that userland has access to it
    raw.span = span

    return messageHandler(
      message,
      properties,
      raw,
      responseHandler.bind(undefined, raw)
    )
  }
}

