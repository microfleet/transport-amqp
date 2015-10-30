const Promise = require('bluebird');
const AMQP = Promise.promisifyAll(require('amqp-coffee'));
const uuid = require('uuid');
const Errors = require('common-errors');
const stringify = require('json-stringify-safe');
const EventEmitter = require('eventemitter3');
const os = require('os');
const ld = require('lodash');
const pkg = require('./package.json');
const { format: fmt } = require('utils');

// serialization functions
const { jsonSerializer, jsonDeserializer } = require('./serialization.js');

class AMQPTransport extends EventEmitter {

  static defaultOpts = {
    name: 'amqp',
    private: false,
    exchange: 'node-services',
    timeout: 3000,
    connection: {
      host: 'localhost',
      port: 5672,
      login: 'guest',
      password: 'guest',
      vhost: '/',
      temporaryChannelTimeout: 6000,
    },
  };

  /**
   * Instantiate AMQP Transport
   * @param  {Object} opts, defaults to {}
   *   - @param {}
   */
  constructor(opts = {}) {
    super();

    // Default configuration
    this._config = ld.merge({}, this.defaultOpts, opts);
    this._replyTo = null;
    this._replyQueue = {};

    // Form app id string for debugging
    this._appID = {
      name: this.cfg.name,
      host: os.hostname(),
      pid: process.pid,
      amqp_version: pkg.version,
      version: opts.version || 'n/a',
    };

    // Cached serialized value
    this._appIDString = stringify(this._appID);
  }

  /**
   * Connects to AMQP, if config.router is specified earlier, automatically invokes .consume function
   * @return {Promise}
   */
  connect() {
    const { amqp, _config: config } = this;

    if (amqp) {
      switch (amqp.state) {
      case 'opening':
      case 'open':
      case 'reconnecting':
        return Promise.reject(new Errors.InvalidOperationError('connection was already initialized, close it first'));
      default:
        // already closed, but make sure
        amqp.close();
        this.amqp = null;
      }
    }

    return Promise.fromNode(function connectedToAMQP(next) {
      this.amqp = new AMQP(config.connection, next);
      this.amqp.on('ready', this._onConnect);
      this.amqp.on('close', this._onClose);
    })
    .then(() => {
      const { router } = config;
      if (router) {
        return this.consume(router);
      }
    })
    .return(this);
  }

  /**
   * Noop function with empty correlation id and reply to data
   * @param  {Error} err
   * @param  {Mixed} data
   */
  noop(error, data) {
    this.emit('log', fmt('when replying to message with %s response could not be delivered', stringify({ error, data }, jsonSerializer)), {
      data,
      err: error,
      type: 'reply',
      level: 'error',
    });
  }

  /**
   * Allows one to consume messages with a given router and predefined callback handler
   * @param  {Function} messageHandler
   * @return {Promise}
   */
  consume(messageHandler) {
    if (!this.amqp) {
      return Promise.reject(new Errors.InvalidOperationError('you must initialize AMQP connection first'));
    }

    function router(message, headers, actions) {
      let next;
      if (!headers.replyTo || !headers.correlationId) {
        next = this.noop;
      } else {
        next = function replyToRequest(error, data) {
          return this.reply(headers, stringify({ error, data }, jsonSerializer)).catch(ld.noop);
        };
      }

      messageHandler(message, headers, actions, next);
    }

    return this
      .createQueue({ queue: this._config.queue || '', neck: this._config.neck, router });
  }

  close() {
    const { amqp } = this;
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
          this.amqp = null;
          amqp.removeAllListeners();
        });

      default:
        this.amqp = null;
        return Promise.resolve();
      }
    }

    return Promise.reject(new Errors.InvalidOperationError('connection was not initialized in the first place'));
  }

  /**
   * Create unnamed private queue (used for reply events)
   */
  createPrivateQueue() {
    this._replyTo = false;

    return this.createQueue({ queue: '', router: this._privateMessageRouter })
      .bind(this)
      .then(function privateQueueCreated(queue, consumer, info) {
        // remove existing listeners
        consumer.removeAllListeners('error');

        // consume errors
        consumer.on('error', (err) => {
          if (err.replyCode === 404 && err.message.indexOf(info.queue) !== -1) {
            // https://github.com/dropbox/amqp-coffee#consumer-event-error
            // handle consumer error on reconnect and close consumer
            // warning: other queues (not private one) should be handled manually
            this.emit('log', fmt('consumer returned 404 error', err), {
              err,
              level: 'warn',
              type: 'consumer',
            });

            // reset replyTo queue
            this._replyTo = false;
            return consumer.close();
          }

          this.emit('error', err);
        });

        // declare _replyTo queueName
        this._replyTo = info.queue;
        this.emit('private-queue-ready');
      });
  }

  /**
   * Create queue with specfied settings in current connection
   * also emit new event on message in queue
   *
   * @param {Object}  _params   - queue parameters
   */
  createQueue(_params) {
    const { amqp } = this;

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
      router: this._messageRouter,
    });

    return amqp
      .queueAsync(params)
      .then(function declareQueue(_channel) {
        channel = _channel;
        return channel.declareAsync();
      })
      .then((_options) => {
        options = _options;
        return Promise.fromNode((next) => {
          consumer = amqp.consume(options.queue, this._queueOpts(params), this._onConsume(params.router), next);
          consumer.on('error', (err) => this.emit('error', err));
        });
      })
      .then(() => {
        this.emit('log', fmt('queue "%s" created', options.queue), {
          queue: options.queue,
          type: 'queue',
          level: 'info',
        });

        return {
          channel,
          consumer,
          options,
        };
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

    const { amqp } = this;
    return amqp.exchangeAsync(params)
      .then(function createdExchange(exchange) {
        return Promise.fromNode(function declareExchange(next) {
          exchange.declare(next);
        });
      })
      .then(() => {
        const { exchange } = params;

        return Promise.map(routes, function bindRoutes(route) {
          return Promise.fromNode(function bindRoute(next) {
            queue.bind(exchange, route, next);
          })
          .tap(() => {
            const queueName = queue.queueOptions.queue;
            this.emit('log', fmt('queue "%s" binded to exchange "%s" on route "%s"', queueName, exchange, route), {
              exchange,
              route,
              queue: queueName,
              level: 'info',
              type: 'queue-bind',
            });
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
    return this.amqp.publish(this._config.exchange, route, message, this._publishOptions(options));
  }

  /**
   * Sends a message and then awaits for response
   * @param  {String} route
   * @param  {Mixed}  message
   * @param  {Object} options
   * @return {Promise}
   */
  publishAndWait(route, message, options = {}) {
    return this._createMessageHandler(options, fmt('job timeout on route "%s" - service does not work or overloaded', route))
      .then(() => {
        return this.publish(route, message, options);
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
    return this.amqp.publish('', queue, message, this._publishOptions(options));
  }

  /**
   * Send message to specified queue directly and wait for answer
   *
   * @param {string} queue        destination queue
   * @param {any} message         message to send
   * @param {object} options      additional options
   */
  sendAndWait(queue, message, options = {}) {
    return this._createMessageHandler(options, fmt('job timeout on queue "%s" - service does not work or overloaded', queue))
      .then(() => {
        return this.send(queue, message, options);
      });
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
  _createMessageHandler(options, errorMessage) {
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

      return promise.return(this).call('_createMessageHandler', options, errorMessage);
    }

    const correlationId = options.correlationId = options.correlationId || uuid.v4();
    options.replyTo = replyTo;

    return new Promise((resolve, reject) => {
      // set timer
      const timeout = options.timeout || this.cfg.timeout;
      const timer = setTimeout(function messageHandlerTimeout() {
        delete this._replyQueue[correlationId];
        timer = null;

        reject(new Errors.TimeoutError(timeout + 'ms'));
      }, timeout);

      this._replyQueue[correlationId] = { resolve, reject, timer };
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
      // do not access .data, because it's a getter and will trigger parses on
      // certain type contents
      const { raw, ack, reject, retry } = message;
      const data = parseInput(raw);

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
      return this.emit('log', fmt('no recipient for the message %s and id %s', message.error || message.data, correlationId), {
        correlationId,
        error: message.error,
        data: message.data,
        log: 'warn',
        type: 'reply',
      });
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
      return {
        err: new Errors.ValidationError('couldn\'t deserialize input', 500, 'message.raw'),
      };
    }
  }

  /**
   * 'ready' event from amqp-coffee lib, perform queue recreation here
   */
  _onConnect = () => {
    const { serverProperties } = this.amqp;
    const { cluster_name, version } = serverProperties;

    // emit connect event through log
    this.emit('log', fmt('connected to %s v%s', cluster_name, version), {
      cluster_name,
      version,
      type: 'connect',
      level: 'info',
    });

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
    this.emit('log', fmt('connection is closed. Had an error:', err ? err : '<n/a>'), {
      type: 'disconnect',
      level: 'warn',
      error: err,
    });

    // re-emit close event
    this.emit('close', err);
  }

}

module.exports = AMQPTransport;
