import is from '@sindresorhus/is'
import Errors from 'common-errors'

export const enum SerializedContentType {
  MSError = 'ms-error',
  Buffer  = 'buffer',
}

export interface SerializedRecord {
  key: any
  value: any
}

export interface Serialized {
  type: SerializedContentType
  data: SerializedRecord[]
}

// generate internal error class for passing between amqp
export const MSError = Errors.helpers.generateClass('MSError', {
  globalize: false,
  args: ['message'],
});

const isSerialized = (it: any): it is Serialized => (
  Object.prototype.hasOwnProperty.call(it, 'data')
)

/**
 * Serializes Own Properties of Error
 * @param  {String} key
 * @returns {Object<{ key, value }>}
 */
function serializeOwnProperties(this: any, key: keyof any): SerializedRecord {
  return {
    key,
    value: this[key],
  }
}

/**
 * Cached Deserialized Own Properties
 * @param  {Object<{ key, value }>} data
 * @returns {Void}
 */
function deserializeOwnProperties(this: any, data: SerializedRecord) {
  this[data.key] = data.value
}

/**
 * Make sure we can transfer errors via rabbitmq through toJSON() call
 * @param  {Error} error
 * @return {Object}
 */
function serializeError(error: Error): Serialized {
  // serialized output
  const serialized: Partial<Serialized> = {
    type: SerializedContentType.MSError,
  };

  serialized.data = Object
    .getOwnPropertyNames(error)
    .map(serializeOwnProperties, error)

  return serialized as Serialized
}

/**
 * Make sure we get a valid JS error
 * @param  {Object} error
 * @return {Error}
 */
function deserializeError(errors: SerializedRecord[]) {
  const error = new MSError()
  errors.forEach(deserializeOwnProperties, error)
  return error
}

export function jsonSerializer(_: string, value: any) {
  if (value instanceof Error) {
    return serializeError(value)
  }

  if (value && value.error instanceof Error) {
    value.error = serializeError(value.error)
  }

  return value
}

export function jsonDeserializer(_: string, value: Serialized | any) {
  if (!is.object(value)) {
    return value
  }

  if (!isSerialized(value)) {
    return value
  }

  const { type, data } = value
  switch (type) {
    case SerializedContentType.MSError:
      return deserializeError(data)

    // TODO
    // case 'Buffer':
    case SerializedContentType.Buffer:
      return Buffer.from(data)

    default:
      return value
  }
}
