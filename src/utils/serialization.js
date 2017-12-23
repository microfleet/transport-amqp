const is = require('is');
const Errors = require('common-errors');

// generate internal error class for passing between amqp
const MSError = Errors.helpers.generateClass('MSError', {
  globalize: false,
  args: ['message'],
});

/**
 * Serializes Own Properties of Error
 * @param  {String} key
 * @returns {Object<{ key, value }>}
 */
function serializeOwnProperties(key) {
  return {
    key,
    value: this[key],
  };
}

/**
 * Cached Deserialized Own Properties
 * @param  {Object<{ key, value }>} data
 * @returns {Void}
 */
function deserializeOwnProperties(data) {
  this[data.key] = data.value;
}

/**
 * Make sure we can transfer errors via rabbitmq through toJSON() call
 * @param  {Error} error
 * @return {Object}
 */
function serializeError(error) {
  // serialized output
  const serialized = {
    type: 'ms-error',
  };

  serialized.data = Object
    .getOwnPropertyNames(error)
    .map(serializeOwnProperties, error);

  return serialized;
}

/**
 * Make sure we get a valid JS error
 * @param  {Object} error
 * @return {Error}
 */
function deserializeError(error) {
  const deserialized = new MSError();
  error.forEach(deserializeOwnProperties, deserialized);
  return deserialized;
}

function jsonSerializer(key, value) {
  if (is.instance(value, Error)) {
    return serializeError(value);
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

    default:
      return value;
  }
}

exports.jsonSerializer = jsonSerializer;
exports.jsonDeserializer = jsonDeserializer;
exports.MSError = MSError;
