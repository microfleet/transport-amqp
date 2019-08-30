const Promise = require('bluebird');
const defaults = require('lodash/defaults');
const is = require('is');
const omit = require('lodash/omit');
const { MSError } = require('./utils/serialization');

/**
 * Connects static helpers to AMQPTransport class
 */
exports = module.exports = (AMQPTransport) => {
  /**
   * Creates AMQPTransport instance
   * @param  {Object} [_config]
   * @param  {Function} [_messageHandler]
   * @returns {AMQPTransport}
   */
  AMQPTransport.create = function create(_config, _messageHandler) {
    let config;
    let messageHandler;

    if (is.fn(_config) && is.undefined(_messageHandler)) {
      messageHandler = _config;
      config = {};
    } else {
      messageHandler = _messageHandler;
      config = _config;
    }

    // init AMQP connection
    const amqp = new AMQPTransport(config);

    // connect & resolve AMQP connector & message handler if it exists
    return amqp.connect().return([amqp, messageHandler]);
  };

  /**
   * Allows one to consume messages with a given router and predefined callback handler
   * @param  {Object} _config
   * @param  {Function} [_messageHandler]
   * @param  {Object} [_opts={}]
   * @returns {Promise<AMQPTransport>}
   */
  AMQPTransport.connect = function connect(config, _messageHandler, _opts = {}) {
    return AMQPTransport
      .create(config, _messageHandler)
      .spread(async (amqp, messageHandler) => {
        // do not init queues
        if (is.fn(messageHandler) !== false || amqp.config.listen) {
          await amqp.createConsumedQueue(messageHandler, amqp.config.listen, _opts);
        }

        return amqp;
      });
  };

  /**
   * Same as AMQPTransport.connect, except that it creates a queue
   * per each of the routes we want to listen to
   * @param  {Object} config
   * @param  {Function} [_messageHandler]
   * @param  {Object} [_opts={}]
   * @returns {Promise<AMQPTransport>}
   */
  AMQPTransport.multiConnect = function multiConnect(config, _messageHandler, opts = []) {
    return AMQPTransport
      .create(config, _messageHandler)
      .spread(async (amqp, messageHandler) => {
        // do not init queues
        if (is.fn(messageHandler) === false && !amqp.config.listen) {
          return amqp;
        }

        await Promise.map(amqp.config.listen, (route, idx) => {
          const queueOpts = opts[idx] || Object.create(null);
          const queueName = config.queue
            ? `${config.queue}-${route.replace(/[#*]/g, '.')}`
            : config.queue;

          const consumedQueueOpts = defaults(queueOpts, {
            queue: queueName,
          });

          return amqp.createConsumedQueue(messageHandler, [route], consumedQueueOpts);
        });

        return amqp;
      });
  };
};

// error data that is going to be copied
const copyErrorData = [
  'code', 'name', 'errors',
  'field', 'reason', 'stack',
];

/**
 * Wraps response error
 * @param {Error} originalError
 * @returns {Error}
 */
exports.wrapError = function wrapError(originalError) {
  if (originalError instanceof Error) {
    return originalError;
  }

  // this only happens in case of .toJSON on error object
  const error = new MSError(originalError.message);

  for (const fieldName of copyErrorData) {
    const mixedData = originalError[fieldName];
    if (mixedData !== undefined && mixedData !== null) {
      error[fieldName] = mixedData;
    }
  }

  return error;
};

/**
 * Set queue opts
 * @param  {Object} opts
 * @return {Object}
 */
exports.setQoS = function setQoS(opts) {
  const { neck } = opts;
  const output = omit(opts, 'neck');

  if (is.undefined(neck)) {
    output.noAck = true;
  } else {
    output.noAck = false;
    output.prefetchCount = neck > 0 ? neck : 0;
  }

  return output;
};
