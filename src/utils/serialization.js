const is = require('is');
const Errors = require('common-errors');

// generate internal error class for passing between amqp

/**
 * @class MSError
 * @param {string} message
 */
const MSError = Errors.helpers.generateClass('MSError', {
  globalize: false,
  args: ['message'],
});

/**
 * Serializes Own Properties of Error
 * @this {Record<string, any>}
 * @param  {string} key
 * @returns {{ key: string, value: any }}
 */
function serializeOwnProperties(key) {
  return {
    key,
    value: this[key],
  };
}

/**
 * Cached Deserialized Own Properties
 * @this {Record<string, any>}
 * @param {{ key: string, value: any }} data
 * @returns {void}
 */
function deserializeOwnProperties(data) {
  this[data.key] = data.value;
}

/**
 * Make sure we can transfer errors via rabbitmq through toJSON() call
 * @param  {Error} error
 * @return {{ type: 'ms-error', data: Record<string, any> }}
 */
function serializeError(error) {
  // serialized output
  return {
    type: 'ms-error',
    data: Object
      .getOwnPropertyNames(error)
      .filter((prop) => typeof error[prop] !== 'function')
      .map(serializeOwnProperties, error),
  };
}

/**
 * Make sure we get a valid JS error
 * @param  {Object} error
 * @return {ReturnType<MSError>}
 */
function deserializeError(error) {
  const deserialized = new MSError();
  error.forEach(deserializeOwnProperties, deserialized);
  return deserialized;
}

/**
 * @param {string} key
 * @param {any} value
 */
function jsonSerializer(key, value) {
  if (value instanceof Error) {
    return serializeError(value);
  }

  if (value && value.error instanceof Error) {
    value.error = serializeError(value.error);
  }

  if (value instanceof Map) {
    return { type: 'map', data: Object.fromEntries(value) };
  }

  if (value instanceof Set) {
    return { type: 'set', data: Array.from(value) };
  }

  return value;
}

function jsonDeserializer(key, value) {
  if (!is.object(value)) {
    return value;
  }

  const { data } = value;
  if (!data) {
    return value;
  }

  const { type } = value;
  switch (type) {
    case 'ms-error':
      return deserializeError(data);

    case 'Buffer':
    case 'buffer':
      return Buffer.from(data);

    case 'ms-set':
      return new Set(data);

    case 'ms-map':
      return new Map(Object.entries(data));

    default:
      return value;
  }
}

exports.jsonSerializer = jsonSerializer;
exports.jsonDeserializer = jsonDeserializer;
exports.MSError = MSError;
