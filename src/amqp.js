// deps
const Promise = require('bluebird');
const uuid = require('uuid');
const Errors = require('common-errors');
const stringify = require('json-stringify-safe');
const EventEmitter = require('eventemitter3');
const os = require('os');
const fmt = require('util').format;
const is = require('is');
const assert = require('assert');

// lodash fp
const merge = require('lodash/merge');
const defaults = require('lodash/defaults');
const noop = require('lodash/noop');
const uniq = require('lodash/uniq');
const omit = require('lodash/omit');
const extend = require('lodash/extend');
const pick = require('lodash/pick');
const set = require('lodash/set');

// local deps
const Joi = require('joi');
const schema = require('./schema');
const pkg = require('../package.json');
const AMQP = require('./utils/transport');
const Cache = require('./utils/cache');
const latency = require('./utils/latency');
const loggerUtils = require('./loggers/levels');
const generateErrorMessage = require('./utils/error');
const { methods } = require('@microfleet/amqp-coffee/bin/src/lib/config').protocol;

// serialization functions
const { jsonSerializer, jsonDeserializer, MSError } = require('./utils/serialization');

// cache references
const { InvalidOperationError, ValidationError } = Errors;
const { AmqpDLXError } = generateErrorMessage;

/**
 * Initializes timeout
 * @param  {Function} reject
 * @param  {number} timeout
 * @param  {string} correlationId
 * @param  {string} routing
 * @returns {Timeout}
 */
const initTimeout = (replyQueue, reject, timeout, correlationId, routing) => {
  replyQueue.delete(correlationId);
  reject(new Errors.TimeoutError(generateErrorMessage(routing, timeout)));
};

/**
 * @class AMQPTransport
 */
class AMQPTransport extends EventEmitter {

  static extendMessageProperties = [
    'deliveryTag',
    'redelivered',
    'exchange',
    'routingKey',
    'weight',
  ];

  static copyErrorData = [
    'code',
    'name',
    'errors',
    'field',
    'reason',
    'stack',
  ];

  /**
   * Instantiate AMQP Transport
   * @param  {Object} opts, defaults to {}
   */
  constructor(opts = {}) {
    super();

    // prepare configuration
    const config = this.config = Joi.attempt(opts, schema);

    // prepares logger
    this.log = loggerUtils.prepareLogger(config);

    // init cache or pass-through operations
    this.cache = new Cache(config.cache);

    // setup instance
    this._replyTo = null;
    this._replyQueue = new Map();
    this._consumers = new WeakMap();
    this._queues = new WeakMap();
    this._boundEmit = this.emit.bind(this);

    // Form app id string for debugging
    this._appID = {
      name: this.config.name,
      host: os.hostname(),
      pid: process.pid,
      utils_version: pkg.version,
      version: opts.version || 'n/a',
    };

    // Cached serialized value
    this._appIDString = stringify(this._appID);
    this._defaultOpts = config.defaultOpts;
    this._extraQueueOptions = {};

    // DLX config
    if (config.dlx.enabled === true) {
      // there is a quirk - we must make sure that no routing key matches queue name
      // to avoid useless redistributions of the message
      this._extraQueueOptions.arguments = { 'x-dead-letter-exchange': config.dlx.params.exchange };
    }
  }

  /**
   * Creates AMQPTransport instance
   * @param  {Object} [_config]
   * @param  {Function} [_messageHandler]
   * @returns {AMQPTransport}
   */
  static create(_config, _messageHandler) {
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
  }

  /**
   * Allows one to consume messages with a given router and predefined callback handler
   * @param  {Object} _config
   * @param  {Function} [_messageHandler]
   * @param  {Object} [_opts={}]
   * @returns {Promise<AMQPTransport>}
   */
  static connect(config, _messageHandler, _opts = {}) {
    return AMQPTransport
      .create(config, _messageHandler)
      .spread((amqp, messageHandler) => {
        // do not init queues
        if (is.fn(messageHandler) === false && !amqp.config.listen) {
          return amqp;
        }

        return amqp.createConsumedQueue(messageHandler, amqp.config.listen, _opts).return(amqp);
      });
  }

  /**
   * Same as AMQPTransport.connect, except that it creates a queue
   * per each of the routes we want to listen to
   * @param  {Object} config
   * @param  {Function} [_messageHandler]
   * @param  {Object} [_opts={}]
   * @returns {Promise<AMQPTransport>}
   */
  static multiConnect(config, _messageHandler, opts = []) {
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
              queue: config.queue ? `${config.queue}-${route}` : config.queue,
            }));
          })
          .return(amqp);
      });
  }

  /**
   * Connects to AMQP, if config.router is specified earlier,
   * automatically invokes .consume function
   * @return {Promise}
   */
  connect() {
    const { _amqp: amqp, config } = this;

    if (amqp) {
      switch (amqp.state) {
        case 'opening':
        case 'open':
        case 'reconnecting': {
          const msg = 'connection was already initialized, close it first';
          const err = new InvalidOperationError(msg);
          return Promise.reject(err);
        }

        default:
          // already closed, but make sure
          amqp.close();
          this._amqp = null;
      }
    }

    return Promise.fromNode((next) => {
      this._amqp = new AMQP(config.connection, next);
      this._amqp.on('ready', this._onConnect);
      this._amqp.on('close', this._onClose);
    })
    .return(this);
  }

  /**
   * Noop function with empty correlation id and reply to data
   * @param  {Error} err
   * @param  {Mixed} data
   */
  noop = (error, data) => {
    const msg = stringify({ error, data }, jsonSerializer);
    this.log.debug('when replying to message with %s response could not be delivered', msg);
  };

  close() {
    const { _amqp: amqp } = this;
    if (amqp) {
      switch (amqp.state) {
        case 'opening':
        case 'open':
        case 'reconnecting':
          return new Promise((resolve, reject) => {
            amqp.once('close', resolve);
            amqp.once('error', reject);
            amqp.close();
          })
          .finally(() => {
            this._amqp = null;
            amqp.removeAllListeners();
          });

        default:
          this._amqp = null;
          return Promise.resolve();
      }
    }

    const err = new InvalidOperationError('connection was not initialized in the first place');
    return Promise.reject(err);
  }

  /**
   * Create queue with specfied settings in current connection
   * also emit new event on message in queue
   *
   * @param {Object}  _params   - queue parameters
   */
  createQueue(_params) {
    const { _amqp: amqp, log, _onConsume } = this;

    // prepare params
    const ctx = {};
    const params = is.string(_params) ? { queue: _params } : _params;
    defaults(params, {
      autoDelete: !params.queue,
      durable: !!params.queue,
    });

    log.debug('initializing queue', params);

    return Promise
      .bind(ctx)
      .then(() => amqp.queueAsync(params))
      .then(function declareQueue(queue) {
        this.queue = queue;
        return queue.declareAsync();
      })
      .catch({ replyCode: 406 }, (err) => {
        log.info('error declaring %s queue: %s', params.queue, err.replyText);
      })
      .catch((err) => {
        log.warn('failed to init queue', params.queue, err.replyText);
        throw err;
      })
      .then(function establishConsumer() {
        // copy queue options
        const options = this.options = { ...this.queue.queueOptions };
        log.info('queue "%s" created', options.queue);

        if (!params.router) {
          return null;
        }

        return Promise.fromNode((next) => {
          log.info('consumer is being created on "%s"', options.queue);

          // setup consumer
          this.consumer = amqp.consume(
            options.queue,
            AMQPTransport._queueOpts(params),
            _onConsume(params.router),
            next
          );
        });
      })
      .return(ctx);
  }

  /**
   * Create unnamed private queue (used for reply events)
   */
  createPrivateQueue() {
    const replyTo = this._replyTo;

    // reset current state
    this._replyTo = false;

    return this
      .createQueue({
        ...this.config.privateQueueOpts,
        autoDelete: true,
        router: this._privateMessageRouter,
        // reuse same private queue name if it was specified before
        queue: replyTo || `microfleet.${uuid.v4()}`,
      })
      .bind(this)
      .then(function privateQueueCreated(data) {
        const { consumer, queue, options } = data;

        // remove existing listeners
        consumer.removeAllListeners('error');
        consumer.removeAllListeners('cancel');

        // consume errors - re-create when we encounter 404
        consumer.on('error', (err) => {
          const error = err.error;
          if (error && error.replyCode === 404 && error.replyText.indexOf(options.queue) !== -1) {
            // https://github.com/dropbox/amqp-coffee#consumer-event-error
            // handle consumer error on reconnect and close consumer
            // warning: other queues (not private one) should be handled manually
            this.log.error('consumer returned 404 error', error);

            // reset replyTo queue and ignore all future errors
            consumer.removeAllListeners('error');
            consumer.removeAllListeners('cancel');
            consumer.on('error', noop);
            consumer.close();

            // recreate queue
            if (this._replyTo !== false) this.createPrivateQueue();

            return null;
          }

          this.log.error('private consumer returned err', err);
          this.emit('error', err);
          return null;
        });

        // re-create on cancel as-well
        consumer.once('cancel', () => {
          consumer.removeAllListeners('error');
          consumer.removeAllListeners('cancel');
          consumer.on('error', noop);
          consumer.close();

          // recreate queue
          if (this._replyTo !== false) this.createPrivateQueue();
        });

        // declare _replyTo queueName
        this._replyTo = options.queue;

        // return data right away
        if (this.config.dlx.enabled !== true) {
          return data;
        }

        // bind temporary queue to direct (?) exchange for DLX messages
        return this
          .bindExchange(queue, this._replyTo, this.config.dlx.params)
          .return(data);
      })
      .tap(() => {
        this.log.debug('private-queue-ready', this._replyTo);
        setImmediate(this._boundEmit, 'private-queue-ready');
      });
  }

  /**
   * Initializes routing function
   * @param  {Function} messageHandler
   * @return {Function}
   */
  static initRoutingFn(messageHandler, transport) {
    return function router(message, headers, actions) {
      let next;

      if (!headers.replyTo || !headers.correlationId) {
        next = transport.noop;
      } else {
        next = function replyToRequest(error, data) {
          return transport.reply(headers, { error, data }).catch(transport.noop);
        };
      }

      return messageHandler(message, headers, actions, next);
    };
  }

  /**
   * Utility function to close consumer and forget about it
   */
  closeConsumer(consumer, next = noop) {
    this.log.warn('closing consumer', consumer.consumerTag);
    consumer.removeAllListeners();
    consumer.on('error', noop);

    // close channel
    consumer.close(() => {
      consumer.waitForMethod(methods.channelClose, () => next());
    });
  }

  /**
   * @param {Function} messageHandler
   * @param {Array} listen
   * @param {Object} options
   */
  createConsumedQueue(messageHandler, listen = [], options = {}) {
    if (is.fn(messageHandler) === false || Array.isArray(listen) === false) {
      throw new Errors.ArgumentError('messageHandler and listen must be present');
    }

    if (is.object(options) === false) {
      throw new Errors.ArgumentError('options');
    }

    const transport = this;
    const config = this.config;
    const queueOptions = merge({
      router: AMQPTransport.initRoutingFn(messageHandler, transport),
      neck: config.neck,
      queue: config.queue || '',
    }, config.defaultQueueOpts, this._extraQueueOptions, options);

    this.log.debug('creating consumed queue %s with routes', queueOptions.queue, listen);

    // bind to an opened exchange once connected
    function createExchange({ queue }) {
      // eslint-disable-next-line no-use-before-define
      const oldQueue = transport._queues.get(establishConsumer) || {};
      const routes = oldQueue._routes || [];

      if (listen.length === 0 && routes.length === 0) {
        queue._routes = [];
        return null;
      }

      // retrieved some of the routes
      transport.log.debug('retrieved routes', routes, listen);

      const rebindRoutes = [...listen, ...routes];
      queue._routes = rebindRoutes;
      return transport.bindExchange(queue, rebindRoutes, config.exchangeArgs);
    }

    // pipeline for establishing consumer
    function establishConsumer() {
      transport.log.debug('[establish consumer]');
      const oldConsumer = transport._consumers.get(establishConsumer);
      let promise = Promise.resolve(transport);

      // if we have old consumer
      if (oldConsumer) {
        promise = promise.tap(() => (
          Promise.fromCallback(next => transport.closeConsumer(oldConsumer, next))
        ));
      }

      return promise
      .call('createQueue', { ...queueOptions })
      .tap(createExchange)
      .catch((e) => {
        throw new Errors.ConnectionError('failed to init queue or exchange', e);
      })
      .then(({ consumer, queue }) => {
        // save ref to WeakMap
        transport._consumers.set(establishConsumer, consumer);
        transport._queues.set(establishConsumer, queue);
        queue._routes = [];

        // invoke to rebind
        function rebind(err, res) {
          const msg = err && err.replyText;

          // cleanup a bit
          transport.log.warn('re-establishing connection after', msg || err, res || '');
          transport.closeConsumer(consumer);

          // if we can't connect - try again in 500 ms in .catch block
          return Promise
            .delay(500)
            .then(establishConsumer)
            .catch(rebind);
        }

        // remove previous listeners if we re-use the channel
        // for any reason
        consumer.removeAllListeners('error');
        consumer.removeAllListeners('cancel');

        // access-refused  403
        //  The client attempted to work with a server entity
        //  to which it has no access due to security settings.
        // not-found  404
        //  The client attempted to work with a server entity that does not exist.
        // resource-locked  405
        //  The client attempted to work with a server entity
        //  to which it has no access because another client is working with it.
        // precondition-failed  406
        //  The client requested a method that was not allowed
        //  because some precondition failed.
        consumer.on('error', (err, res) => {
          const error = err.error || err;

          // https://www.rabbitmq.com/amqp-0-9-1-reference.html -
          switch (error.replyCode) {
            // ignore errors
            case 311:
            case 313:
              transport.log.error('error working with a channel:', err, res);
              return null;

            case 404:
              if (error.replyText && error.replyText.indexOf(queue.queueOptions.queue) !== -1) {
                rebind(error, res);
              }
              return null;

            default:
              return rebind(error, res);
          }
        });

        consumer.on('cancel', rebind);

        // emit event that we consumer & queue is ready
        transport.log.info('[consumed-queue-reconnected] %s', queue.queueOptions.queue);
        transport.emit('consumed-queue-reconnected', consumer, queue);

        return [consumer, queue, establishConsumer];
      })
      .catch(Errors.ConnectionError, (e) => {
        transport.log.warn('[consumed-queue-down]', e);
        return Promise.delay(500).then(establishConsumer);
      });
    }

    // make sure we recreate queue and establish consumer on reconnect
    return establishConsumer().tap(() => {
      transport.log.debug('bound `ready` to establishConsumer for', listen, queueOptions.queue);
      transport.on('ready', establishConsumer);
    });
  }

  /**
   * Stops consumed queue from reestablishing connection
   * @returns {Promise<*>}
   */
  stopConsumedQueue(consumer, bindFn, next = noop) {
    this.removeListener('ready', bindFn);
    this.closeConsumer(consumer, next);
  }

  /**
   * Bind specified queue to exchange
   *
   * @param {object} queue   - queue instance created by .createQueue
   * @param {string} _routes - messages sent to this route will be delivered to queue
   * @param {object} params  - exchange parameters:
   *                 https://github.com/dropbox/amqp-coffee#connectionexchangeexchangeargscallback
   */
  bindExchange(queue, _routes, params = {}) {
    // make sure we have an expanded array of routes
    const routes = Array.isArray(_routes) ? uniq(_routes) : [_routes];

    // default params
    defaults(params, {
      exchange: this.config.exchange,
      type: this.config.exchangeArgs.type,
      durable: true,
    });

    // empty exchange
    const { exchange } = params;
    if (!exchange) {
      const err = new ValidationError('please specify exchange name', 500, 'params.exchange');
      return Promise.reject(err);
    }

    this.log.debug('bind routes->exchange', routes, exchange);

    return this
    ._amqp
    .exchangeAsync(params)
    .call('declareAsync')
    .catch({ replyCode: 406 }, (err) => {
      const format = '[406] error declaring exchange with params %s: %s';
      this.log.warn(format, JSON.stringify(params), err.replyText);
    })
    .return(routes)
    .map(route => (
      queue.bindAsync(exchange, route).tap(() => {
        const queueName = queue.queueOptions.queue;
        if (queue._routes) {
          // reconnect might push an extra route
          if (queue._routes.indexOf(route) === -1) {
            queue._routes.push(route);
          }

          this.log.trace('[queue routes]', queue._routes);
        }

        this.log.debug('queue "%s" bound to exchange "%s" on route "%s"', queueName, exchange, route);
      })
    ));
  }

  /**
   * Unbind specified queue from exchange
   *
   * @param {object} queue   - queue instance created by .createQueue
   * @param {string} _routes - messages sent to this route will be delivered to queue
   */
  unbindExchange(queue, _routes) {
    const exchange = this.config.exchange;
    const routes = Array.isArray(_routes) ? _routes : [_routes];

    return Promise.map(routes, route => (
      queue.unbindAsync(exchange, route).tap(() => {
        const queueName = queue.queueOptions.queue;
        if (queue._routes) {
          const idx = queue._routes.indexOf(route);
          if (idx >= 0) {
            queue._routes.splice(idx, 1);
          }

          this.log.debug('queue routes', queue._routes);
        }

        this.log.info('queue "%s" unbound from exchange "%s" on route "%s"', queueName, exchange, route);
      })
    ));
  }

  /**
   * Send message to specified route
   *
   * @param   {String} route   - destination route
   * @param   {Mixed}  message - message to send - will be coerced to string via stringify
   * @param   {Object} options - additional options
   */
  publish(route, message, options = {}) {
    // prepare exchange
    const exchange = is.string(options.exchange)
      ? options.exchange
      : this.config.exchange;

    return this._amqp.publishAsync(
      exchange,
      route,
      stringify(message, jsonSerializer),
      this._publishOptions(options)
    );
  }

  /**
   * Sends a message and then awaits for response
   * @param  {String} route
   * @param  {Mixed}  message
   * @param  {Object} options
   * @return {Promise}
   */
  publishAndWait(route, message, options = {}) {
    const time = process.hrtime();
    return this._createMessageHandler(
      route,
      message,
      options,
      this.publish
    )
    .tap(() => {
      this.log.trace('publishAndWait took %s ms', latency(time));
    });
  }

  /**
   * Send message to specified queue directly
   *
   * @param {String} queue     - destination queue
   * @param {Mixed}  message   - message to send
   * @param {Object} [options] - additional options
   */
  send(queue, message, options = {}) {
    return this._amqp.publishAsync(
      options.exchange || '',
      queue,
      stringify(message, jsonSerializer),
      this._publishOptions(options)
    );
  }

  /**
   * Send message to specified queue directly and wait for answer
   *
   * @param {string} queue        destination queue
   * @param {any} message         message to send
   * @param {object} options      additional options
   */
  sendAndWait(queue, message, options = {}) {
    return this._createMessageHandler(
      queue,
      message,
      options,
      this.send
    );
  }

  /**
   * Reply to sender queue based on headers
   *
   * @param   {Object} headers - incoming message headers
   * @param   {Mixed}  message - message to send
   */
  reply(headers, message) {
    if (!headers.replyTo || !headers.correlationId) {
      const err = new ValidationError('replyTo and correlationId not found in headers', 400);
      return Promise.reject(err);
    }

    return this.send(headers.replyTo, message, { correlationId: headers.correlationId });
  }

  /**
   * Creates local listener for when a private queue is up
   * @returns {Promise<Void|Error>}
   */
  _awaitPrivateQueue() {
    /* eslint-disable prefer-const */
    return new Promise((resolve, reject) => {
      let done;
      let error;

      done = function onReady() {
        this.removeAllListeners('error', error);
        error = null;
        resolve();
      };

      error = function onError(err) {
        this.removeListener('private-queue-ready', done);
        done = null;
        reject(err);
      };

      this.once('private-queue-ready', done);
      this.once('error', error);
    });
    /* eslint-enable prefer-const */
  }

  /**
   * Creates response message handler and sets timeout on the response
   * @param  {Object} options
   * @param  {String} errorMessage
   * @return {Promise}
   */
  _createMessageHandler(routing, message, options, publishMessage) {
    const replyTo = options.replyTo || this._replyTo;
    const time = process.hrtime();

    if (!replyTo) {
      let promise;
      if (replyTo === false) {
        promise = this._awaitPrivateQueue();
      } else {
        promise = this.createPrivateQueue();
      }

      return promise
        .return(this)
        .call('_createMessageHandler', routing, message, options, publishMessage)
        .tap(() => {
          this.log.debug('private queue created in %s', latency(time));
        });
    }

    /**
     * Process cached responses
     */
    let hashKey;
    const maxAge = options.cache;
    if (maxAge) {
      const cachedResponse = this.cache.get(message, maxAge);
      if (cachedResponse !== null) {
        if (typeof cachedResponse === 'object') {
          return Promise.resolve(cachedResponse.value);
        }

        hashKey = cachedResponse;
      }
    }

    // set timer
    const correlationId = options.correlationId || uuid.v4();
    const timeout = options.timeout || this.config.timeout;
    const replyQueue = this._replyQueue;

    // slightly longer timeout, if message was not consumed in time, it will return with expiration
    return new Promise((resolve, reject) => {
      const timer = setTimeout(initTimeout, timeout, replyQueue, reject, timeout, correlationId, routing);

      // push into queue
      replyQueue.set(correlationId, { resolve, reject, timer, time, cache: hashKey });

      // debugging
      this.log.trace('message pushed into reply queue in %s', latency(time));

      // add custom header for routing over amq.headers exchange
      set(options, 'headers.x-reply-to', replyTo);

      // this is to ensure that queue is not overflown and work will not
      // be completed later on
      publishMessage.call(this, routing, message, {
        ...options,
        replyTo,
        correlationId,
        expiration: Math.ceil(timeout * 0.9).toString(),
      })
      .tap(() => {
        this.log.trace('message published in %s', latency(time));
      })
      .catch((err) => {
        this.log.error('error sending message', err);
        clearTimeout(timer);
        replyQueue.delete(correlationId);
        reject(err);
      });

      return null;
    });
  }

  /**
   * Set queue opts
   * @param  {Object} opts
   * @return {Object}
   */
  static _queueOpts(opts) {
    const { neck } = opts;
    const output = omit(opts, 'neck');

    if (is.undefined(neck)) {
      output.noAck = true;
    } else {
      output.noAck = false;
      output.prefetchCount = neck > 0 ? neck : 0;
    }

    return output;
  }

  /**
   * Specifies default publishing options
   * @param  {Object} options
   * @param  {String} options.exchange - will be overwritten by exchange thats passed
   *  in the publish/send methods
   *  https://github.com/dropbox/amqp-coffee/blob/6d99cf4c9e312c9e5856897ab33458afbdd214e5/src/lib/Publisher.coffee#L90
   * @return {Object}
   */
  _publishOptions(options = {}) {
    const opts = {
      ...options,
      appId: this._appIDString,
    };

    defaults(opts, this._defaultOpts);

    // append request timeout in headers
    defaults(opts.headers, {
      timeout: opts.timeout || this.config.timeout,
    });

    return opts;
  }

  /**
   *
   * @param  {Object} message
   *   - @param {Object} data: a getter that returns the data in its parsed form, eg a
   *                           parsed json object, a string, or the raw buffer
   *  - @param {Object} raw: the raw buffer that was returned
   *  - @param {Object} properties: headers specified for the message
   *  - @param {Number} size: message body size
   *  - @param {Function} ack(): function : only used when prefetchCount is specified
   *  - @param {Function} reject(): function: only used when prefetchCount is specified
   *  - @param {Function} retry(): function: only used when prefetchCount is specified
   */
  _onConsume = (router) => {
    const parseInput = this._parseInput;
    const amqpTransport = this;

    assert(is.fn(router), '`router` must be a function');

    return function consumeMessage(message) {
      // pick extra properties
      extend(message.properties, pick(message, AMQPTransport.extendMessageProperties));

      // do not access .data, because it's a getter and will trigger parses on
      // certain type contents
      const data = parseInput.call(amqpTransport, message.raw);

      // pass to the message router
      // data - headers - actions
      router.call(amqpTransport, data, message.properties, message);
    };
  }

  /**
   * Distributes messages from a private queue
   * @param  {Mixed}  message
   * @param  {Object} properties
   * @param  {Object} actions
   */
  _privateMessageRouter(message, properties) {
    const { correlationId, replyTo, headers } = properties;
    const { 'x-death': xDeath } = headers;
    const future = this._replyQueue.get(correlationId);

    if (!future) {
      this.log.error('no recipient for the message %j and id %s', message.error || message.data || message, correlationId);

      if (xDeath) {
        this.log.warn('message was not processed', message, xDeath);
        return null;
      }

      if (replyTo) {
        const msg = fmt('no recipients found for message with correlation id %s', correlationId);
        return this.reply(properties, { error: new Errors.NotPermittedError(msg) });
      }

      // mute
      return null;
    }

    this.log.trace('response returned in %s', latency(future.time));

    clearTimeout(future.timer);
    this._replyQueue.delete(correlationId);

    // if messag was dead-lettered - reject with an error
    if (xDeath) {
      return future.reject(new AmqpDLXError(xDeath, message));
    }

    if (message.error) {
      const { error: originalError } = message;
      let error;

      if (originalError instanceof Error) {
        error = originalError;
      } else {
        // this only happens in case of .toJSON on error object
        error = new MSError(originalError.message);
        AMQPTransport.copyErrorData.forEach((fieldName) => {
          const mixedData = originalError[fieldName];
          if (is.defined(mixedData) && is.nil(mixedData) === false) {
            error[fieldName] = mixedData;
          }
        });
      }

      return future.reject(error);
    }

    // retrieve data
    const data = message.data;

    // if cache key is set - try to save it
    const cacheKey = future.cache;
    if (cacheKey) {
      this.cache.set(cacheKey, data);
    }

    return future.resolve(data);
  }

  /**
   * Parses AMQP message
   * @param  {Buffer} _data
   * @return {Object}
   */
  _parseInput(_data) {
    try {
      return JSON.parse(_data, jsonDeserializer);
    } catch (err) {
      this.log.warn('Error parsing buffer', err, _data.toString());
      return {
        err: new Errors.ValidationError('couldn\'t deserialize input', 500, 'message.raw'),
      };
    }
  }

  /**
   * 'ready' event from amqp-coffee lib, perform queue recreation here
   */
  _onConnect = () => {
    const { serverProperties, connection } = this._amqp;
    const { cluster_name, version } = serverProperties;

    // setup connection
    if ('setNoDelay' in connection) {
      connection.setNoDelay();
    }

    // emit connect event through log
    this.log.info('connected to %s v%s', cluster_name, version);

    // https://github.com/dropbox/amqp-coffee#reconnect-flow
    // recreate unnamed private queue
    if ((this._replyTo || this.config.private) && this._replyTo !== false) {
      this.createPrivateQueue();
    }

    // re-emit ready
    this.emit('ready');
  };

  /**
   * Pass in close event
   */
  _onClose = (err) => {
    // emit connect event through log
    this.log.error('connection is closed. Had an error:', err || '<n/a>');

    // re-emit close event
    this.emit('close', err);
  };
}

module.exports = AMQPTransport;
