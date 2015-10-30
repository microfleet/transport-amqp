const Errors = require('common-errors');
const MSError = Errors.helpers.generateClass('MSError', {
  globalize: false,
  args: [ 'message' ],
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
    .map(function transferErrorContext(key) {
      return { key, value: error[key] };
    });

  return serialized;
}

/**
 * Make sure we get a valid JS error
 * @param  {[type]} error [description]
 * @return {[type]}       [description]
 */
function deserializeError(error) {
  const deserialized = new MSError();
  error.forEach(function transferSerializedErrorContext(data) {
    deserialized[data.key] = data.value;
  });

  Errors.prependCurrentStack(deserialized);
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
