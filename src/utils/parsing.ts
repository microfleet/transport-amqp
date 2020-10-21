import { promisify } from 'util'
import { gunzip } from 'zlib'

import { ContentEncoding, ContentType } from '../types'
import { kParseError } from '../constants'
import { jsonDeserializer } from './serialization'
import { LoggerLike } from '../schema/logger-like'

const gunzipAsync = promisify(gunzip)

export const safeJSONParse = (data: any, log?: LoggerLike) => {
  try {
    return JSON.parse(data, jsonDeserializer)
  } catch (err) {
    if (log) {
      log.warn('Error parsing buffer', err, String(data))
    }

    return { err: kParseError }
  }
}

/**
 * Parses AMQP message
 * @param  {Buffer} _data
 * @param  {String} [contentType='application/json']
 * @param  {String} [contentEncoding='plain']
 * @return {Object}
 */
export async function parseInput(
  this: { log?: LoggerLike } | null,
  data: Buffer,
  contentType: ContentType = ContentType.Json,
  contentEncoding: ContentEncoding = ContentEncoding.Plain,
){
  let content

  switch (contentEncoding) {
    case ContentEncoding.Gzip:
      try {
        content = await gunzipAsync(data)
      } catch (e) {
        return { err: kParseError }
      }
      break

    case ContentEncoding.Plain:
      content = data
      break

    default:
      return { err: kParseError }
  }

  switch (contentType) {
    // default encoding when we were pre-stringifying and sending str
    // and our updated encoding when we send buffer now
    case ContentType.Utf8:
    case ContentType.Json:
      return safeJSONParse(content, this?.log)

    default:
      return content
  }
}
