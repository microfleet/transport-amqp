const Promise = require('bluebird');
const noop = require('lodash/noop');
const defaults = require('lodash/defaults');
const is = require('is');
const omit = require('lodash/omit');
const { methods } = require('@microfleet/amqp-coffee/bin/src/lib/config').protocol;
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
      .spread((amqp, messageHandler) => {
        // do not init queues
        if (is.fn(messageHandler) === false && !amqp.config.listen) {
          return amqp;
        }

        return amqp.createConsumedQueue(messageHandler, amqp.config.listen, _opts).return(amqp);
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
      .spread((amqp, messageHandler) => {
        // do not init queues
        if (is.fn(messageHandler) === false && !amqp.config.listen) {
          return amqp;
        }

        return Promise
          .resolve(amqp.config.listen)
          .map((route, idx) => {
            const queueOpts = opts[idx] || {};
            return amqp.createConsumedQueue(messageHandler, [route], defaults(queueOpts, {
              queue: config.queue ? `${config.queue}-${route.replace(/[#*]/g, '.')}` : config.queue,
            }));
          })
          .return(amqp);
      });
  };
};

/**
 * Utility function to close consumer and forget about it
 */
exports.closeConsumer = function closeConsumer(consumer) {
  this.log.warn('closing consumer', consumer.consumerTag);
  consumer.removeAllListeners();
  consumer.on('error', noop);

  // close channel
  return Promise.fromCallback(done => (
    consumer.close(() => {
      consumer.waitForMethod(methods.channelClose, () => done());
    })
  ));
};

/**
 * Routing function HOC with reply RPC enhancer
 * @param  {Function} messageHandler
 * @returns {Function}
 */
exports.initRoutingFn = function initRoutingFn(messageHandler, transport) {
  return function router(message, properties, actions) {
    let next;

    if (!properties.replyTo || !properties.correlationId) {
      next = transport.noop;
    } else {
      next = function replyToRequest(error, data) {
        return transport.reply(properties, { error, data }).catch(transport.noop);
      };
    }

    return messageHandler(message, properties, actions, next);
  };
};

// error data that is going to be copied
const copyErrorData = [
  'code', 'name', 'errors',
  'field', 'reason', 'stack',
];

/**
 * Wraps response error
 * @param  {Error} error
 * @returns {Error}
 */
exports.wrapError = function wrapError(originalError) {
  if (originalError instanceof Error) {
    return originalError;
  }

  // this only happens in case of .toJSON on error object
  const error = new MSError(originalError.message);

  // eslint-disable-next-line no-restricted-syntax
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
