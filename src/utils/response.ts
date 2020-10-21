import { MessageHeaders, PublishingOpts } from '../types'

export interface Message<
  Body extends any,
  $Error extends Error = Error
> {
  data: Body
  error?: $Error
}

export interface Response<
  Body extends any,
  Headers extends MessageHeaders = MessageHeaders
> {
  data: Body,
  headers: Headers
}

export interface ReplyOptions {
  simpleResponse?: boolean
}

/**
 * @param {Object} response
 * @oaram {Object} response.data
 * @oaram {Object} response.headers
 * @param {Object} replyOptions
 * @param {boolean} replyOptions.simpleResponse
 * @returns {Object}
 */
export function adaptResponse<Body extends any>(
  response: Body | Response<Body>,
  replyOptions: ReplyOptions
): Body {
  return replyOptions.simpleResponse === false
    ? response as Body
    : (response as Response<Body>).data
}

/**
 * @param {mixed} message
 * @param {Object} message.data
 * @param {Object} message.error
 * @param {Object} properties
 * @param {Object} properties.headers
 */
export function buildResponse<
  Body extends any,
  Properties extends PublishingOpts = PublishingOpts,
>(
  message: Message<Body>,
  properties: Properties
) {
  const { headers } = properties
  const { data } = message

  return {
    headers,
    data,
  }
}
