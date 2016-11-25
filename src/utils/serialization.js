const is = require('is');
const Errors = require('common-errors');

// generate internal error class for passing between amqp
const MSError = Errors.helpers.generateClass('MSError', {
  globalize: false,
  args: ['message'],
});

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
    .map(key => ({ key, value: error[key] }));

  return serialized;
}

/**
 * Make sure we get a valid JS error
 * @param  {Object} error
 * @return {Error}
 */
function deserializeError(error) {
  const deserialized = new MSError();
  error.forEach((data) => {
    deserialized[data.key] = data.value;
  });

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

  const data = value.data;
  if (!data) {
    return value;
  }

  const type = value.type;
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
