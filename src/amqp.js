// deps
const Promise = require('bluebird');
const uuid = require('node-uuid');
const Errors = require('common-errors');
const stringify = require('json-stringify-safe');
const EventEmitter = require('eventemitter3');
const os = require('os');
const fmt = require('util').format;
const is = require('is');
const ld = require('lodash').runInContext();
const Bunyan = require('bunyan');
const blackhole = require('bunyan-noop');

// local deps
const pkg = require('../package.json');
const validator = require('./utils/validator.js');
const AMQP = require('./utils/transport.js');
const latency = require('./utils/latency.js');
const generateErrorMessage = require('./utils/error.js');

// serialization functions
const { jsonSerializer, jsonDeserializer, MSError } = require('./utils/serialization.js');
const { InvalidOperationError, ValidationError } = Errors;

/**
 * @class AMQPTransport
 */
class AMQPTransport extends EventEmitter {

  static defaultOpts = require('./defaults.js');

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
  ];

  /**
   * Instantiate AMQP Transport
   * @param  {Object} opts, defaults to {}
   */
  constructor(opts = {}) {
    super();
    const config = this._config = ld.merge({}, AMQPTransport.defaultOpts, opts);

    // default to array
    if (is.string(config.listen)) {
      config.listen = [config.listen];
    }

    // validate configuration
    const { error } = validator.validateSync('amqp', config);
    if (error) throw error;

    // bunyan logger
    if (config.debug && !config.log) {
      this.log = require('./utils/logger.js');
    } else {
      this.log = is.instance(config.log, Bunyan) ? config.log : blackhole();
    }

    // setup instance
    this._replyTo = null;
    this._replyQueue = new Map();
    this._consumers = new WeakMap();
    this._queues = new WeakMap();

    // Form app id string for debugging
    this._appID = {
      name: this._config.name,
      host: os.hostname(),
      pid: process.pid,
      utils_version: pkg.version,
      version: opts.version || 'n/a',
    };

    // Cached serialized value
    this._appIDString = stringify(this._appID);
    this._defaultOpts = config.defaultOpts;
  }

  /**
   * Allows one to consume messages with a given router and predefined callback handler
   * @param  {Function} messageHandler
   * @return {Promise}
   */
  static connect(_config, _messageHandler, _opts = {}) {
    let config;
    let messageHandler;

    if (is.fn(_config) && is.undefined(_messageHandler)) {
      messageHandler = _config;
      config = {};
    } else {
      messageHandler = _messageHandler;
      config = _config;
    }

    const amqp = new AMQPTransport(config);

    return amqp.connect()
      .tap(() => {
        if (is.fn(messageHandler) === false && !amqp._config.listen) {
          return null;
        }

        return amqp.createConsumedQueue(messageHandler, amqp._config.listen, _opts);
      })
      .return(amqp);
  }

  /**
   * Connects to AMQP, if config.router is specified earlier,
   * automatically invokes .consume function
   * @return {Promise}
   */
  connect() {
    const { _amqp: amqp, _config: config } = this;

    if (amqp) {
      switch (amqp.state) {
        case 'opening':
        case 'open':
        case 'reconnecting':
          const msg = 'connection was already initialized, close it first';
          const err = new InvalidOperationError(msg);
          return Promise.reject(err);
        default:
          // already closed, but make sure
          amqp.close();
          this._amqp = null;
      }
    }

    return Promise.fromNode(next => {
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
    const { _amqp: amqp } = this;

    let queue;
    let consumer;
    let options;

    // basis for params
    const params = is.string(_params) ? { queue: _params } : _params;

    ld.defaults(params, {
      autoDelete: !params.queue,
      durable: !!params.queue,
    });

    return amqp
      .queueAsync(params)
      .then(_queue => {
        queue = _queue;
        return queue.declareAsync();
      })
      .catch({ replyCode: 406 }, err => {
        this.log.info('error declaring %s queue: %s', params.queue, err.replyText);
        return queue.queueOptions;
      })
      .catch(err => {
        this.log.warn('failed to init queue', params.queue, err.replyText);
        throw err;
      })
      .then(_options => {
        options = { ..._options };
        this.log.info('queue "%s" created', options.queue);

        if (!params.router) {
          return null;
        }

        return Promise.fromNode(next => {
          this.log.info('consumer is being created on "%s"', options.queue);

          // setup consumer
          consumer = amqp.consume(
            options.queue,
            this._queueOpts(params),
            this._onConsume(params.router),
            next
          );

          // TODO: should be done somewhere else?
          // add error handling
          // consumer.on('error', err => this.emit('error', err));
        });
      })
      .then(() => ({ queue, consumer, options }));
  }

  /**
   * Create unnamed private queue (used for reply events)
   */
  createPrivateQueue() {
    this._replyTo = false;

    return this
      .createQueue({
        ...this._config.privateQueueOpts,
        queue: '',
        router: this._privateMessageRouter,
      })
      .bind(this)
      .then(function privateQueueCreated(data) {
        const { consumer, options } = data;

        // remove existing listeners
        consumer.removeAllListeners('error');

        // consume errors
        consumer.on('error', err => {
          if (err.replyCode === 404 && err.message.indexOf(options.queue) !== -1) {
            // https://github.com/dropbox/amqp-coffee#consumer-event-error
            // handle consumer error on reconnect and close consumer
            // warning: other queues (not private one) should be handled manually
            this.log.error('consumer returned 404 error', err);

            // reset replyTo queue
            this._replyTo = false;
            return consumer.close();
          }

          this.emit('error', err);
          return null;
        });

        // declare _replyTo queueName
        this._replyTo = options.queue;
        setImmediate(() => {
          this.emit('private-queue-ready');
        });

        return data;
      });
  }

  /**
   * Initializes routing function
   * @param  {Function} messageHandler
   * @return {Function}
   */
  initRoutingFn(messageHandler, transport) {
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
  closeConsumer(consumer) {
    this.log.warn('closing consumer', consumer.consumerTag);
    consumer.removeAllListeners();

    if (consumer.state !== 'closed') {
      consumer.on('error', ld.noop);
      consumer.close();
    } else {
      consumer.on('error', error => {
        this.log.error('closed consumer error', error);
      });
    }
  }

  /**
   * @param {Function} messageHandler
   * @param {Array} listen
   * @param {Object} options
   */
  createConsumedQueue(messageHandler, listen = [], options = {}) {
    if (ld.isFunction(messageHandler) === false && Array.isArray(listen) === false) {
      throw new Errors.ArgumentError('messageHandler or listen must be present');
    }

    if (ld.isPlainObject(options) === false) {
      throw new Errors.ArgumentError('options');
    }

    const transport = this;
    const config = this._config;
    const queueOptions = ld.merge({
      router: transport.initRoutingFn(messageHandler, transport),
      neck: config.neck,
      queue: config.queue || '',
    }, config.defaultQueueOpts, options);

    // bind to an opened exchange once connected
    function createExchange({ queue }) {
      // eslint-disable-next-line no-use-before-define
      const oldQueue = transport._queues.get(establishConsumer) || {};
      const routes = oldQueue._routes || [];

      if (listen.length === 0 && routes.length === 0) {
        queue._routes = [];
        return null;
      }

      const rebindRoutes = [...listen, ...routes];
      queue._routes = rebindRoutes;
      return transport.bindExchange(queue, rebindRoutes, config.exchangeArgs);
    }

    // pipeline for establishing consumer
    function establishConsumer() {
      transport.log.debug('[establish consumer]');
      const oldConsumer = transport._consumers.get(establishConsumer);
      if (oldConsumer) {
        transport.closeConsumer(oldConsumer);
      }

      return transport
      .createQueue({ ...queueOptions })
      .tap(createExchange)
      .then(({ consumer, queue }) => {
        // save ref to WeakMap
        transport._consumers.set(establishConsumer, consumer);
        transport._queues.set(establishConsumer, queue);
        queue._routes = [];

        // invoke to rebind
        function rebind(err, res) {
          const msg = err && err.replyCode;

          // cleanup a bit
          transport.log.warn('re-establishing connection after', msg || err, res || '');
          transport.closeConsumer(consumer);

          // if we can't connect - try again in 500 ms in .catch block
          return Promise
            .delay(500)
            .then(establishConsumer)
            .catch(rebind);
        }

        // access-refused	403
        //  The client attempted to work with a server entity
        //  to which it has no access due to security settings.
        // not-found	404
        //  The client attempted to work with a server entity that does not exist.
        // resource-locked	405
        //  The client attempted to work with a server entity
        //  to which it has no access because another client is working with it.
        // precondition-failed	406
        //  The client requested a method that was not allowed
        //  because some precondition failed.
        consumer.on('error', (err, res) => {
          // https://www.rabbitmq.com/amqp-0-9-1-reference.html -
          switch (err.replyCode) {
            // ignore errors
            case 311:
            case 313:
              transport.log.error('error working with a channel:', err, res);
              return null;

            default:
              return rebind(err, res);
          }
        });

        consumer.on('cancel', rebind);

        // emit event that we consumer & queue is ready
        transport.log.info('[consumed-queue-reconnected] %s', queue.queueOptions.queue);
        transport.emit('consumed-queue-reconnected', consumer, queue);

        return [consumer, queue];
      });
    }

    // make sure we recreate queue and establish consumer on reconnect
    return establishConsumer().tap(() => {
      transport.on('ready', establishConsumer);
    });
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
    const routes = Array.isArray(_routes) ? _routes : [_routes];

    // default params
    ld.defaults(params, {
      exchange: this._config.exchange,
      type: this._config.exchangeArgs.type,
      durable: true,
    });

    // empty exchange
    const { exchange } = params;
    if (!exchange) {
      const err = new ValidationError('please specify exchange name', 500, 'params.exchange');
      return Promise.reject(err);
    }

    return this
    ._amqp
    .exchangeAsync(params)
    .call('declareAsync')
    .catch({ replyCode: 406 }, err => {
      const format = '[406] error declaring exchange with params %s: %s';
      this.log.warn(format, JSON.stringify(params), err.replyText);
    })
    .then(() => Promise
      .map(routes, route => queue
      .bindAsync(exchange, route)
      .tap(() => {
        const queueName = queue.queueOptions.queue;
        if (queue._routes) {
          queue._routes.push(route);
          this.log.trace('[queue routes]', queue._routes);
        }

        const format = 'queue "%s" binded to exchange "%s" on route "%s"';
        this.log.debug(format, queueName, exchange, route);
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
    const exchange = this._config.exchange;
    const routes = Array.isArray(_routes) ? _routes : [_routes];

    return Promise.map(
      routes,
      route => queue.unbindAsync(exchange, route)
        .tap(() => {
          const queueName = queue.queueOptions.queue;
          if (queue._routes) {
            const idx = queue._routes.indexOf(route);
            if (idx >= 0) {
              queue._routes.splice(idx, 1);
            }
          }

          this.log.info(
            'queue "%s" unbinded from exchange "%s" on route "%s"',
            queueName,
            exchange,
            route
          );
        })
    );
  }

  /**
   * Send message to specified route
   *
   * @param   {String} route   - destination route
   * @param   {Mixed}  message - message to send - will be coerced to string via stringify
   * @param   {Object} options - additional options
   */
  publish(route, message, options = {}) {
    return this._amqp.publishAsync(
      options.exchange || this._config.exchange,
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
   * @return {Promise}
   */
  _awaitPrivateQueue() {
    /* eslint-disable prefer-const */
    return new Promise((resolve, reject) => {
      let done;
      let error;

      done = function onReady() {
        this.removeAllListeners('error', error);
        resolve(this);
      };

      error = function onError(err) {
        this.removeListener('private-queue-ready', done);
        reject(err);
      };

      this.once('private-queue-ready', done);
      this.once('error', error);
    });
    /* eslint-enable prefer-const */
  }

  _initTimeout(reject, timeout, correlationId, routing) {
    return setTimeout(() => {
      this._replyQueue.delete(correlationId);
      reject(new Errors.TimeoutError(generateErrorMessage(routing, timeout)));
    }, timeout);
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

    // slightly longer timeout, if message was not consumed in time, it will return with expiration
    return new Promise((resolve, reject) => {
      // set timer
      const correlationId = options.correlationId || uuid.v4();
      const timeout = options.timeout || this._config.timeout;
      const timer = this._initTimeout(reject, timeout, correlationId, routing);

      // push into queue
      this._replyQueue.set(correlationId, { resolve, reject, timer, time });

      this.log.trace('message pushed into reply queue in %s', latency(time));

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
      .catch(err => {
        this.log.error('error sending message', err);
        clearTimeout(timer);
        this._replyQueue.delete(correlationId);
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
  _queueOpts(opts) {
    const { neck } = opts;
    const output = ld.omit(opts, 'neck');

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
   *                                   in the publish/send methods
   *                                   https://github.com/dropbox/amqp-coffee/blob/6d99cf4c9e312c9e5856897ab33458afbdd214e5/src/lib/Publisher.coffee#L90
   * @return {Object}
   */
  _publishOptions(options = {}) {
    const opts = {
      ...options,
      appId: this._appIDString,
    };

    ld.defaults(opts, this._defaultOpts);

    // append request timeout in headers
    ld.defaults(opts.headers, {
      timeout: opts.timeout || this._config.timeout,
    });

    return opts;
  }

  /**
   *
   * @param  {Object} message
   * 	- @param {Object} data: a getter that returns the data in its parsed form, eg a
   * 													parsed json object, a string, or the raw buffer
   *  - @param {Object} raw: the raw buffer that was returned
   *  - @param {Object} properties: headers specified for the message
   *  - @param {Number} size: message body size
   *  - @param {Function} ack(): function : only used when prefetchCount is specified
   *  - @param {Function} reject(): function: only used when prefetchCount is specified
   *  - @param {Function} retry(): function: only used when prefetchCount is specified
   */
  _onConsume(router) {
    const parseInput = this._parseInput;
    const amqpTransport = this;

    return function consumeMessage(message) {
      // pick extra properties
      ld.extend(message.properties, ld.pick(message, AMQPTransport.extendMessageProperties));

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
   * @param  {Object} headers
   * @param  {Object} actions
   */
  _privateMessageRouter(message, headers) {
    const { correlationId } = headers;
    const future = this._replyQueue.get(correlationId);

    if (!future) {
      this.log.error(
        'no recipient for the message %s and id %s',
        message.error || message.data || message,
        correlationId
      );

      if (headers.replyTo) {
        const msg = fmt('no recipients found for message with correlation id %s', correlationId);
        return this.reply(headers, {
          error: new Errors.NotPermittedError(msg),
        });
      }

      // mute
      return null;
    }

    this.log.trace('response returned in %s', latency(future.time));

    clearTimeout(future.timer);
    this._replyQueue.delete(correlationId);

    if (message.error) {
      const { error: originalError } = message;
      let error;

      if (originalError instanceof Error) {
        error = originalError;
      } else {
        // this only happens in case of .toJSON on error object
        error = new MSError(originalError.message);
        AMQPTransport.copyErrorData.forEach(fieldName => {
          const mixedData = originalError[fieldName];
          if (is.defined(mixedData) && is.nil(mixedData) === false) {
            error[fieldName] = mixedData;
          }
        });
      }

      return future.reject(error);
    }

    return future.resolve(message.data);
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
    const { serverProperties } = this._amqp;
    const { cluster_name, version } = serverProperties;

    // emit connect event through log
    this.log.info('connected to %s v%s', cluster_name, version);

    // https://github.com/dropbox/amqp-coffee#reconnect-flow
    // recreate unnamed private queue
    if (this._replyTo || this._config.private) {
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
