const Promise = require('bluebird');
const AMQP = require('amqp-coffee');
const uuid = require('node-uuid');
const Errors = require('common-errors');
const stringify = require('json-stringify-safe');
const EventEmitter = require('eventemitter3');
const os = require('os');
const ld = require('lodash');
const pkg = require('../package.json');
const path = require('path');
const { format: fmt } = require('util');

// init validation
const Validation = require('ms-validation');
const validator = new Validation('..', function filterFiles(filename) {
  return path.extname(filename) === '.json' && path.basename(filename, '.json') !== 'package';
});

// serialization functions
const { jsonSerializer, jsonDeserializer } = require('./serialization.js');

class AMQPTransport extends EventEmitter {

  static defaultOpts = {
    name: 'amqp',
    private: false,
    exchange: 'node-services',
    timeout: 10000,
    debug: process.env.NODE_ENV === 'development',
    connection: {
      host: 'localhost',
      port: 5672,
      login: 'guest',
      password: 'guest',
      vhost: '/',
      temporaryChannelTimeout: 6000,
    },
  };

  static extendMessageProperties = [
    'deliveryTag',
    'redelivered',
    'exchange',
    'routingKey',
    'weight',
  ];

  /**
   * Instantiate AMQP Transport
   * @param  {Object} opts, defaults to {}
   *   - @param {}
   */
  constructor(opts = {}) {
    super();
    const config = this._config = ld.merge({}, AMQPTransport.defaultOpts, opts);

    // validate configuration
    const { error } = validator.validateSync('amqp', config);
    if (error) {
      throw error;
    }

    // setup instance
    this._replyTo = null;
    this._replyQueue = {};

    // add simple debugger
    if (this._config.debug) {
      this.on('log', (message) => {
        process.stdout.write('> ' + message + '\n');
      });
    }

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
  }

  /**
   * Allows one to consume messages with a given router and predefined callback handler
   * @param  {Function} messageHandler
   * @return {Promise}
   */
  static connect(_config, _messageHandler) {
    let config;
    let messageHandler;

    if (typeof _config === 'function' && typeof _messageHandler === 'undefined') {
      messageHandler = _config;
      config = {};
    } else {
      messageHandler = _messageHandler;
      config = _config;
    }

    const amqp = new AMQPTransport(config);

    return amqp
      .connect()
      .tap(function establishQueuesAndExchanges() {
        let channelPromise;
        if (typeof messageHandler === 'function' || amqp._config.listen) {
          const router = function router(message, headers, actions) {
            let next;
            if (!headers.replyTo || !headers.correlationId) {
              next = amqp.noop;
            } else {
              next = function replyToRequest(error, data) {
                return amqp.reply(headers, { error, data }).catch(amqp.noop);
              };
            }

            messageHandler(message, headers, actions, next);
          };

          // open channel for communication
          channelPromise = amqp.createQueue({ queue: amqp._config.queue || '', neck: amqp._config.neck, router });

          if (amqp._config.listen) {
            // open exchange when we need to listen to routes
            channelPromise.then(function createExchange(queueData) {
              const { channel } = queueData;
              return amqp.bindExchange(channel, amqp._config.listen);
            });
          }
        }

        return channelPromise;
      })
      .return(amqp);
  }

  /**
   * Connects to AMQP, if config.router is specified earlier, automatically invokes .consume function
   * @return {Promise}
   */
  connect() {
    const { _amqp: amqp, _config: config } = this;

    if (amqp) {
      switch (amqp.state) {
      case 'opening':
      case 'open':
      case 'reconnecting':
        return Promise.reject(new Errors.InvalidOperationError('connection was already initialized, close it first'));
      default:
        // already closed, but make sure
        amqp.close();
        this._amqp = null;
      }
    }

    return Promise.fromNode((next) => {
      this._amqp = Promise.promisifyAll(new AMQP(config.connection, next));
      this._amqp.on('ready', this._onConnect);
      this._amqp.on('close', this._onClose);
      return this;
    });
  }

  /**
   * Noop function with empty correlation id and reply to data
   * @param  {Error} err
   * @param  {Mixed} data
   */
  noop = (error, data) => {
    if (this.listeners('log', true)) {
      this.log('when replying to message with %s response could not be delivered', stringify({ error, data }, jsonSerializer));
    }
  }

  log = (...opts) => {
    if (this.listeners('log', true)) {
      this.emit('log', fmt(...opts));
    }
  }

  close() {
    const { _amqp: amqp } = this;
    if (amqp) {
      switch (amqp.state) {
      case 'opening':
      case 'open':
      case 'reconnecting':
        return new Promise(function disconnectListener(resolve, reject) {
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

    return Promise.reject(new Errors.InvalidOperationError('connection was not initialized in the first place'));
  }

  /**
   * Create queue with specfied settings in current connection
   * also emit new event on message in queue
   *
   * @param {Object}  _params   - queue parameters
   */
  createQueue(_params) {
    const { _amqp: amqp } = this;

    let channel;
    let consumer;
    let options;
    let params;

    if (typeof _params === 'string') {
      params = { queue: _params };
    } else {
      params = _params;
    }

    ld.defaults(params, {
      autoDelete: !params.queue,
      durable: !!params.queue,
    });

    return amqp
      .queueAsync(params)
      .then(function declareQueue(_channel) {
        channel = _channel;
        return Promise.fromCallback(function declareQueueCallback(next) {
          channel.declare(next);
        });
      })
      .then((_options) => {
        options = _options;

        this.log('queue "%s" created', options.queue);

        if (!params.router) {
          return null;
        }

        return Promise.fromNode((next) => {
          this.log('consumer is being created on "%s"', options.queue);

          consumer = amqp.consume(options.queue, this._queueOpts(params), this._onConsume(params.router), next);
          consumer.on('error', (err) => this.emit('error', err));
        });
      })
      .then(() => {
        return {
          channel,
          consumer,
          options,
        };
      });
  }

  /**
   * Create unnamed private queue (used for reply events)
   */
  createPrivateQueue() {
    this._replyTo = false;

    return this.createQueue({ queue: '', router: this._privateMessageRouter })
      .bind(this)
      .then(function privateQueueCreated({ consumer, options }) {
        // remove existing listeners
        consumer.removeAllListeners('error');

        // consume errors
        consumer.on('error', (err) => {
          if (err.replyCode === 404 && err.message.indexOf(options.queue) !== -1) {
            // https://github.com/dropbox/amqp-coffee#consumer-event-error
            // handle consumer error on reconnect and close consumer
            // warning: other queues (not private one) should be handled manually
            this.log('consumer returned 404 error', err);

            // reset replyTo queue
            this._replyTo = false;
            return consumer.close();
          }

          this.emit('error', err);
        });

        // declare _replyTo queueName
        this._replyTo = options.queue;
        this.emit('private-queue-ready');
      });
  }


  /**
   * Bind specified queue to exchange
   *
   * @param {object} queue   - queue instance created by .createQueue
   * @param {string} _routes - messages sent to this route will be delivered to queue
   * @param {object} params  - exchange parameters: https://github.com/dropbox/amqp-coffee#connectionexchangeexchangeargscallback
   */
  bindExchange(queue, _routes, params = {}) {
    // make sure we have an expanded array of routes
    const routes = Array.isArray(_routes) ? _routes : [ _routes ];

    // default params
    ld.defaults(params, {
      exchange: this._config.exchange,
      type: 'topic',
      durable: true,
    });

    // empty exchange
    if (!params.exchange) {
      return Promise.reject(new Errors.ValidationError('please specify exchange name', 500, 'params.exchange'));
    }

    const { _amqp: amqp } = this;
    return amqp
      .exchangeAsync(params)
      .then(function createdExchange(exchange) {
        return Promise.fromNode(function declareExchange(next) {
          exchange.declare(next);
        });
      })
      .then(() => {
        const { exchange } = params;

        return Promise.resolve(routes).bind(this).map(function bindRoutes(route) {
          return Promise.fromNode(function bindRoute(next) {
            queue.bind(exchange, route, next);
          })
          .tap(() => {
            const queueName = queue.queueOptions.queue;
            this.log('queue "%s" binded to exchange "%s" on route "%s"', queueName, exchange, route);
          });
        });
      });
  }

  /**
   * Send message to specified route
   *
   * @param   {String} route   - destination route
   * @param   {Mixed}  message - message to send - will be coerced to string via stringify
   * @param   {Object} options - additional options
   */
  publish(route, message, options = {}) {
    return this._amqp.publishAsync(this._config.exchange, route, stringify(message, jsonSerializer), this._publishOptions(options));
  }

  /**
   * Sends a message and then awaits for response
   * @param  {String} route
   * @param  {Mixed}  message
   * @param  {Object} options
   * @return {Promise}
   */
  publishAndWait(route, message, options = {}) {
    return this._createMessageHandler(
      options,
      fmt('job timeout on route "%s" - service does not work or overloaded', route),
      (opts = {}) => {
        return this.publish(route, message, ld.merge(opts, options));
      }
    );
  }

  /**
   * Send message to specified queue directly
   *
   * @param {String} queue     - destination queue
   * @param {Mixed}  message   - message to send
   * @param {Object} [options] - additional options
   */
  send(queue, message, options = {}) {
    return this._amqp.publishAsync('', queue, stringify(message, jsonSerializer), this._publishOptions(options));
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
      options,
      fmt('job timeout on queue "%s" - service does not work or overloaded', queue),
      (opts = {}) => {
        return this.send(queue, message, ld.merge(opts, options));
      }
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
      return Promise.reject(new Errors.ValidationError('replyTo and correlationId not found in headers', 400));
    }

    return this.send(headers.replyTo, message, { correlationId: headers.correlationId });
  }

  /**
   * Creates response message handler and sets timeout on the response
   * @param  {Object} options
   * @param  {String} errorMessage
   * @return {Promise}
   */
  _createMessageHandler(options, errorMessage, publishMessage) {
    const replyTo = options.replyTo || this._replyTo;
    let promise;

    if (!replyTo) {
      if (replyTo === false) {
        promise = new Promise((resolve, reject) => {
          let done;
          let error;

          done = function onReady() {
            this.removeAllListener('error', error);
            resolve(this);
          };

          error = function onError(err) {
            this.removeListener('private-queue-ready', done);
            reject(err);
          };

          this.once('private-queue-ready', done);
          this.once('error', error);
        });
      } else {
        promise = this.createPrivateQueue();
      }

      return promise
        .return(this)
        .call('_createMessageHandler', options, errorMessage, publishMessage);
    }

    const correlationId = options.correlationId = options.correlationId || uuid.v4();
    options.replyTo = replyTo;

    return new Promise((resolve, reject) => {
      // set timer
      const timeout = options.timeout || this._config.timeout;
      const timer = setTimeout(() => {
        delete this._replyQueue[correlationId];
        reject(new Errors.TimeoutError(timeout + 'ms: ' + errorMessage));
      }, timeout); // slightly longer timeout, if message was not consumed in time, it will return with expiration

      this._replyQueue[correlationId] = { resolve, reject, timer };

      // this is to ensure that queue is not overflown and work will not
      // be completed later on
      return publishMessage({ expiration: Math.ceil(timeout * 0.9).toString() });
    });
  }

  /**
   * Set queue opts
   * @param  {Object} opts
   * @return {Object}
   */
  _queueOpts(opts) {
    const { neck } = opts;
    if (typeof neck === 'undefined') {
      opts.noAck = true;
    } else {
      opts.noAck = false;
      opts.prefetchCount = neck > 0 ? neck : 0;
    }

    return ld.omit(opts, 'neck');
  }

  /**
   * Specifies default publishing options
   * @param  {Object} options
   * @return {Object}
   */
  _publishOptions(options = {}) {
    // set application id
    options.appId = this._appIDString;
    options.headers = options.headers || {};

    // append request timeout in headers
    ld.defaults(options.headers, {
      timeout: options.timeout || this._config.timeout,
    });

    ld.defaults(options, {
      confirm: true,
      mandatory: true,
    });

    return options;
  }

  /**
   *
   * @param  {Object} message
   * 	- @param {Object} data: a getter that returns the data in its parsed form, eg a parsed json object, a string, or the raw buffer
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

      // emit log
      amqpTransport.log('Incoming message:', String(message.raw), message.properties);

      // do not access .data, because it's a getter and will trigger parses on
      // certain type contents
      const { raw, ack, reject, retry } = message;
      const data = parseInput.call(amqpTransport, raw);

      // pass to the message router
      // data - headers - actions
      router.call(amqpTransport, data, message.properties, { ack, reject, retry });
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
    const future = this._replyQueue[correlationId];

    if (!future) {
      this.log('no recipient for the message %s and id %s', message.error || message.data, correlationId);

      if (headers.replyTo) {
        return this.reply(headers, {
          error: new Errors.NotPermittedError(fmt('no recipients found for message with correlation id %s', correlationId)),
        });
      }

      // mute
      return null;
    }

    clearTimeout(future.timer);
    delete this._replyQueue[correlationId];

    if (message.error) {
      return future.reject(message.error);
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
      this.log('Error parsing buffer', err, _data.toString());
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
    this.log('connected to %s v%s', cluster_name, version);

    // https://github.com/dropbox/amqp-coffee#reconnect-flow
    // recreate unnamed private queue
    if (this._replyTo || this._config.private) {
      this.createPrivateQueue();
    }

    // re-emit ready
    this.emit('ready');
  }

  /**
   * Pass in close event
   */
  _onClose = (err) => {
    // emit connect event through log
    this.log('connection is closed. Had an error:', err ? err : '<n/a>');

    // re-emit close event
    this.emit('close', err);
  }

}

module.exports = AMQPTransport;
