const Errors = require('common-errors');
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
    .map(key => ({
      key,
      value: error[key],
    }));

  return serialized;
}

/**
 * Make sure we get a valid JS error
 * @param  {Object} error
 * @return {Error}
 */
function deserializeError(error) {
  const deserialized = new MSError();
  error.forEach(data => {
    deserialized[data.key] = data.value;
  });

  return deserialized;
}

function jsonSerializer(key, value) {
  if (value && typeof value === 'object' && value instanceof Error) {
    return serializeError(value);
  }

  return value;
}

function jsonDeserializer(key, value) {
  if (value && typeof value === 'object') {
    const { data, type } = value;
    if (data) {
      if (type === 'ms-error') {
        return deserializeError(data);
      } else if (type === 'buffer') {
        return new Buffer(data);
      }
    }
  }

  return value;
}

exports.jsonSerializer = jsonSerializer;
exports.jsonDeserializer = jsonDeserializer;
exports.MSError = MSError;
