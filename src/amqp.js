// deps
const Promise = require('bluebird');
const gunzip = Promise.promisify(require('zlib').gunzip);
const gzip = Promise.promisify(require('zlib').gzip);
const uuid = require('uuid');
const flatstr = require('flatstr');
const stringify = require('json-stringify-safe');
const EventEmitter = require('eventemitter3');
const os = require('os');
const is = require('is');
const assert = require('assert');
const opentracing = require('opentracing');
const {
  ConnectionError,
  NotPermittedError,
  ValidationError,
  InvalidOperationError,
  ArgumentError,
} = require('common-errors');

// lodash fp
const merge = require('lodash/merge');
const defaults = require('lodash/defaults');
const noop = require('lodash/noop');
const uniq = require('lodash/uniq');
const pick = require('lodash/pick');

// local deps
const { Joi, schema } = require('./schema');
const pkg = require('../package.json');
const AMQP = require('./utils/transport');
const ReplyStorage = require('./utils/reply-storage');
const Backoff = require('./utils/recovery');
const Cache = require('./utils/cache');
const latency = require('./utils/latency');
const loggerUtils = require('./loggers');
const generateErrorMessage = require('./utils/error');
const helpers = require('./helpers');
const { kReplyHeaders } = require('./constants');

// serialization functions
const { jsonSerializer, jsonDeserializer } = require('./utils/serialization');

// cache references
const { AmqpDLXError } = generateErrorMessage;
const { wrapError, setQoS } = helpers;
const { Tags, FORMAT_TEXT_MAP } = opentracing;

/**
 * Wraps regular in a bluebird promise
 * @param  {Span} span opentracing span
 * @param  {Promise} promise pending promise
 *
 * @return {BluebirdPromise}
 */
const wrapPromise = (span, promise) => Promise.resolve((async () => {
  try {
    return await promise;
  } catch (error) {
    span.setTag(Tags.ERROR, true);
    span.log({
      event: 'error',
      'error.object': error,
      message: error.message,
      stack: error.stack,
    });

    throw error;
  } finally {
    span.finish();
  }
})());

const serialize = async (message, publishOptions) => {
  let serialized;
  switch (publishOptions.contentType) {
    case 'application/json':
    case 'string/utf8':
      serialized = Buffer.from(flatstr(stringify(message, jsonSerializer)));
      break;

    default:
      throw new Error('invalid content-type');
  }

  if (publishOptions.contentEncoding === 'gzip') {
    return gzip(serialized);
  }

  return serialized;
};

function safeJSONParse(data, log) {
  try {
    return JSON.parse(data, jsonDeserializer);
  } catch (err) {
    log.warn('Error parsing buffer', err, String(data));
    return { err: PARSE_ERR };
  }
}

const toUniqueStringArray = (routes) => (
  Array.isArray(routes) ? uniq(routes) : [routes]
);

/**
 * Routing function HOC with reply RPC enhancer
 * @param  {Function} messageHandler
 * @param  {AMQPTransport} transport
 * @returns {Function}
 */
const initRoutingFn = (messageHandler, transport) => {
  /**
   * Response Handler Function. Sends Reply or Noop log.
   * @param  {AMQPMessage} raw - Raw AMQP Message Structure
   * @param  {Error} error - Error if it happened.
   * @param  {mixed} data - Response data.
   * @returns {Promise<*>}
   */
  function responseHandler(raw, error, data) {
    const { properties, span } = raw;
    return !properties.replyTo || !properties.correlationId
      ? transport.noop(error, data, span, raw)
      : transport.reply(properties, { error, data }, span, raw);
  }

  /**
   * Initiates consumer message handler.
   * @param  {mixed} message - RequestBody passed from the publisher.
   * @param  {Object} properties - AMQP Message properties.
   * @param  {Object} raw - Original AMQP message.
   * @param  {Function} [raw.ack] - Acknowledge if nack is `true`.
   * @param  {Function} [raw.reject] - Reject if nack is `true`.
   * @param  {Function} [raw.retry] - Retry msg if nack is `true`.
   * @returns {Void}
   */
  return function router(message, properties, raw) {
    // add instrumentation
    const appId = safeJSONParse(properties.appId, this.log);

    // opentracing instrumentation
    const childOf = this.tracer.extract(FORMAT_TEXT_MAP, properties.headers || {});
    const span = this.tracer.startSpan(`onConsume:${properties.routingKey}`, {
      childOf,
    });

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_RPC_SERVER,
      [Tags.PEER_SERVICE]: appId.name,
      [Tags.PEER_HOSTNAME]: appId.host,
    });

    // define span in the original message
    // so that userland has access to it
    raw.span = span;

    return messageHandler(message, properties, raw, responseHandler.bind(undefined, raw));
  };
};

/**
 * @param {Object} response
 * @oaram {Object} response.data
 * @oaram {Object} response.headers
 * @param {Object} replyOptions
 * @param {boolean} replyOptions.simpleResponse
 * @returns {Object}
 */
function adaptResponse(response, replyOptions) {
  return replyOptions.simpleResponse === false ? response : response.data;
}

/**
 * @param {mixed} message
 * @param {Object} message.data
 * @param {Object} message.error
 * @param {Object} properties
 * @param {Object} properties.headers
 */
function buildResponse(message, properties) {
  const { headers } = properties;
  const { data } = message;

  return {
    headers,
    data,
  };
}

const extendMessageProperties = [
  'deliveryTag',
  'redelivered',
  'exchange',
  'routingKey',
  'weight',
];

const error406 = { replyCode: 406 };

/**
 * @class AMQPTransport
 */
class AMQPTransport extends EventEmitter {
  /**
   * Instantiate AMQP Transport
   * @param  {Object} opts, defaults to {}
   */
  constructor(opts = {}) {
    super();

    // prepare configuration
    const config = this.config = Joi.attempt(opts, schema, {
      allowUnknown: true,
    });

    this.config = config;

    // prepares logger
    this.log = loggerUtils.prepareLogger(config);
    this.log.debug({ config }, 'used configuration');

    // init cache or pass-through operations
    this.cache = new Cache(config.cache);

    // reply storage, where we'd save correlation ids
    // and callbacks to be called once we are done
    this.replyStorage = new ReplyStorage();

    // delay settings for reconnect
    this.recovery = new Backoff(config.recovery);

    // init open tracer - default one is noop
    this.tracer = config.tracer || new opentracing.Tracer();

    // setup instance
    this._replyTo = null;
    this._consumers = new Map();
    this._queues = new WeakMap();
    this._reconnectionHandlers = new WeakMap();
    this._boundEmit = this.emit.bind(this);
    this._onConsume = this._onConsume.bind(this);
    this._on406 = this._on406.bind(this);
    this._onClose = this._onClose.bind(this);
    this._onConnect = this._onConnect.bind(this);

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
    this._defaultOpts = { ...config.defaultOpts };
    this._defaultOpts.appId = this._appIDString;
    this._extraQueueOptions = {};

    // DLX config
    if (config.dlx.enabled === true) {
      // there is a quirk - we must make sure that no routing key matches queue name
      // to avoid useless redistributions of the message
      this._extraQueueOptions.arguments = { 'x-dead-letter-exchange': config.dlx.params.exchange };
    }
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

    return Promise
      .fromNode((next) => {
        this._amqp = new AMQP(config.connection, next);
        this._amqp.on('ready', this._onConnect);
        this._amqp.on('close', this._onClose);
      })
      .return(this);
  }

  /**
   * Noop function with empty correlation id and reply to data
   * @param  {Error} error
   * @param  {mixed} data
   * @param  {Span}  [span]
   * @param  {AMQPMessage} [raw]
   */
  noop(error, data, span, raw) {
    const msg = stringify({ error, data }, jsonSerializer);
    this.log.debug('when replying to message with %s response could not be delivered', msg);

    if (span !== undefined) {
      if (error) {
        span.setTag(Tags.ERROR, true);
        span.log({
          event: 'error', 'error.object': error, message: error.message, stack: error.stack,
        });
      }

      span.finish();
    }

    if (raw !== undefined) {
      this.emit('after', raw);
    }
  }

  /**
   * Stops consumers and closes transport
   */
  async _close() {
    const { _amqp: amqp } = this;

    await this.closeAllConsumers();

    try {
      await new Promise((resolve, reject) => {
        amqp.once('close', resolve);
        amqp.once('error', reject);
        amqp.close();
      });
    } finally {
      this._amqp = null;
      amqp.removeAllListeners();
    }
  }

  close() {
    const { _amqp: amqp } = this;

    if (amqp) {
      switch (amqp.state) {
        case 'opening':
        case 'open':
        case 'reconnecting':
          return this._close();
        default:
          this._amqp = null;
          return Promise.resolve();
      }
    }

    const err = new InvalidOperationError('connection was not initialized in the first place');
    return Promise.reject(err);
  }

  /**
   * Create queue with specified settings in current connection
   * also emit new event on message in queue
   *
   * @param {Object}  opts   - queue parameters
   */
  async createQueue(opts) {
    const { _amqp: amqp, log, _onConsume } = this;

    // prepare params
    const ctx = Object.create(null);
    const userParams = is.string(opts) ? { queue: opts } : opts;
    const requestedName = userParams.queue;
    const params = merge({ autoDelete: !requestedName, durable: !!requestedName }, userParams);

    log.debug({ params }, 'initializing queue');

    const queue = ctx.queue = await amqp.queueAsync(params);

    await Promise
      .resolve(queue.declareAsync())
      .catch(error406, this._on406.bind(this, params))
      .tapCatch((err) => {
        log.warn({ err, queue: params.queue }, 'failed to init queue');
      });

    // copy queue options
    const options = ctx.options = { ...ctx.queue.queueOptions };
    const queueName = options.queue;
    log.info({ queue: queueName }, 'queue created');

    if (!params.router) {
      return ctx;
    }

    log.info({ queue: options.queue }, 'consumer is being created');

    // setup consumer
    const messageHandler = _onConsume(params.router);
    ctx.consumer = await amqp.consumeAsync(queueName, setQoS(params), messageHandler);

    return ctx;
  }

  handlePrivateConsumerError(consumer, queue, err) {
    const { error } = err;
    if (error && error.replyCode === 404 && error.replyText.indexOf(queue) !== -1) {
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

      return;
    }

    this.log.error('private consumer returned err', err);
    this.emit('error', err);
  }

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
  async handleConsumerError(consumer, queue, err, res) {
    const error = err.error || err;

    // https://www.rabbitmq.com/amqp-0-9-1-reference.html -
    switch (error.replyCode) {
      // ignore errors
      case 311:
      case 313:
        this.log.error({ err, res }, 'error working with a channel:');
        return null;

      case 404:
        if (error.replyText && error.replyText.includes(queue.queueOptions.queue)) {
          return this.rebindConsumer(consumer, error, res);
        }
        return null;

      default:
        this.log.warn({ err }, 'unhandled consumer error');
        return this.rebindConsumer(consumer, error, res);
    }
  }

  async rebindConsumer(consumer, err, res) {
    const msg = err ? err.replyText : 'uncertain';

    // cleanup a bit
    this.log.warn({ err, res }, 're-establishing connection after %s', msg);

    // saved reference to re-establishing function
    const establishConsumer = this._reconnectionHandlers.get(consumer);

    if (establishConsumer == null) {
      this.log.fatal({ err, res }, 'failed to fetch connection handler');
      return;
    }

    try {
      await this.closeConsumer(consumer);
      await Promise.delay(this.recovery.get('consumed', 1));
    } catch (e) {
      this.log.error({ err: e }, 'failed to close consumer');
    } finally {
      await establishConsumer();
    }
  }

  handlePrivateConsumerCancel(consumer) {
    consumer.removeAllListeners('error');
    consumer.removeAllListeners('cancel');
    consumer.on('error', noop);
    consumer.close();

    // recreate queue unless it is already being recreated
    if (this._replyTo !== false) {
      this.createPrivateQueue();
    }
  }

  /**
   * Create unnamed private queue (used for reply events)
   */
  async createPrivateQueue(attempt = 0) {
    const replyTo = this._replyTo;
    const queueOpts = {
      ...this.config.privateQueueOpts,
      router: this._privateMessageRouter, // private router here
      queue: replyTo || `microfleet.${uuid.v4()}`, // reuse same private queue name if it was specified before
    };

    // reset current state
    this._replyTo = false;

    let createdQueue;
    try {
      const { consumer, queue, options } = createdQueue = await this.createQueue(queueOpts);

      // remove existing listeners
      consumer.removeAllListeners('error');
      consumer.removeAllListeners('cancel');

      // consume errors - re-create when we encounter 404 or on cancel
      consumer.on('error', this.handlePrivateConsumerError.bind(this, consumer, options.queue));
      consumer.once('cancel', this.handlePrivateConsumerCancel.bind(this, consumer));

      // declare _replyTo queueName
      this._replyTo = options.queue;

      // bind temporary queue to headers exchange for DLX messages
      // NOTE: if this fails we might have a problem where expired messages
      // are not delivered & private queue is never ready
      const dlxConfig = this.config.dlx;
      if (dlxConfig.enabled === true) {
        await this.bindHeadersExchange(queue, this._replyTo, dlxConfig.params, 'reply-to');
      }
    } catch (e) {
      this.log.error('private queue creation failed - restarting', e);
      await Promise.delay(this.recovery.get('private', attempt));
      return this.createPrivateQueue(attempt + 1);
    }

    this.log.debug({ queue: this._replyTo }, 'private-queue-ready');
    setImmediate(this._boundEmit, 'private-queue-ready');

    return createdQueue;
  }

  async bindQueueToExchangeOnRoutes(routes, queue, oldQueue = Object.create(null)) {
    const previousRoutes = oldQueue._routes || [];

    if (routes.length === 0 && previousRoutes.length === 0) {
      queue._routes = [];
      return;
    }

    // retrieved some of the routes
    this.log.debug({ routes, previousRoutes }, 'retrieved routes');

    const rebindRoutes = uniq([...previousRoutes, ...routes]);
    queue._routes = rebindRoutes;

    const work = [
      this.bindExchange(queue, rebindRoutes, this.config.exchangeArgs),
    ];

    // bind same queue to headers exchange
    if (this.config.bindPersistantQueueToHeadersExchange === true) {
      work.push(this.bindHeadersExchange(queue, rebindRoutes, this.config.headersExchange));
    }

    await Promise.all(work);
  }

  /**
   * @param {Function} messageHandler
   * @param {Array} listen
   * @param {Object} options
   */
  async createConsumedQueue(messageHandler, listen = [], options = {}) {
    if (is.fn(messageHandler) === false || Array.isArray(listen) === false) {
      throw new ArgumentError('messageHandler and listen must be present');
    }

    if (is.object(options) === false) {
      throw new ArgumentError('options');
    }

    const { config } = this;
    const router = initRoutingFn(messageHandler, this);
    const baseOpts = { router, neck: config.neck, queue: config.queue || '' };
    const queueOptions = merge(baseOpts, config.defaultQueueOpts, this._extraQueueOptions, options);

    if (config.bindPersistantQueueToHeadersExchange === true) {
      for (const route of listen.values()) {
        assert.ok(
          /^[^*#]+$/, route,
          'with bindPersistantQueueToHeadersExchange: true routes must not have patterns'
        );
      }
    }

    // pipeline for establishing consumer
    const establishConsumer = async (attempt = 0) => {
      const { log, recovery } = this;
      const {
        _consumers: consumers,
        _queues: queues,
        _reconnectionHandlers: connectionHandlers,
      } = this;

      log.debug({ attempt }, 'establish consumer');

      const oldConsumer = consumers.get(establishConsumer);
      const oldQueue = queues.get(establishConsumer);

      // if we have old consumer
      if (oldConsumer) {
        await this.closeConsumer(oldConsumer);
      }

      let createdQueue;
      try {
        const { queue } = createdQueue = await this.createQueue({ ...queueOptions });
        await this.bindQueueToExchangeOnRoutes(listen, queue, oldQueue);
      } catch (e) {
        const err = new ConnectionError('failed to init queue or exchange', e);
        log.warn({ err }, '[consumed-queue-down]');
        await Promise.delay(recovery.get('consumed', attempt + 1));
        return establishConsumer(attempt + 1);
      }

      const { consumer, queue } = createdQueue;

      // save ref to WeakMap
      consumers.set(establishConsumer, consumer);
      queues.set(establishConsumer, queue);
      connectionHandlers.set(consumer, establishConsumer);

      // remove previous listeners if we re-use the channel
      // for any reason
      consumer.removeAllListeners('error');
      consumer.removeAllListeners('cancel');

      consumer.on('error', this.handleConsumerError.bind(this, consumer, queue));
      consumer.on('cancel', this.rebindConsumer.bind(this, consumer));

      // emit event that we consumer & queue is ready
      const queueName = queue.queueOptions.queue;
      log.info({ queueName, consumerTag: consumer.consumerTag }, 'consumed-queue-reconnected');
      this.emit('consumed-queue-reconnected', consumer, queue, establishConsumer);

      return queue.queueOptions.queue;
    };

    // make sure we recreate queue and establish consumer on reconnect
    this.log.debug({ listen, queue: queueOptions.queue }, 'creating consumed queue');
    const queueName = await establishConsumer();

    this.log.debug({ listen, queue: queueName }, 'bound `ready` to establishConsumer');
    this.on('ready', establishConsumer);

    return establishConsumer;
  }

  /**
   * Stops current running consumers
   */
  async closeAllConsumers() {
    const work = [];
    for (const consumer of this._consumers.values()) {
      work.push(this.stopConsumedQueue(consumer));
    }
    await Promise.all(work);
  }

  /**
   * Utility function to close consumer and forget about it
   */
  async closeConsumer(consumer) {
    this.log.warn('closing consumer', consumer.consumerTag);

    // cleanup after one-self
    this._consumers.delete(this._reconnectionHandlers.get(consumer));
    this._reconnectionHandlers.delete(consumer);

    consumer.removeAllListeners();
    consumer.on('error', noop);

    this._boundEmit('consumer-close', consumer);

    // close channel
    await Promise
      .fromCallback((done) => {
        consumer.cancel(done);
      })
      .timeout(5000)
      .catch(Promise.TimeoutError, noop);

    this.log.info({ consumerTag: consumer.consumerTag }, 'closed consumer');
  }

  /**
   * Prevents consumer from re-establishing connection
   * @param {Consumer} consumer
   * @returns {Promise<Void>}
   */
  async stopConsumedQueue(consumer) {
    if (!consumer) {
      throw new TypeError('consumer must be defined');
    }

    const establishConsumer = this._reconnectionHandlers.get(consumer);
    this.log.debug({ establishConsumer: !!establishConsumer }, 'fetched establish consumer');
    if (establishConsumer) {
      this.removeListener('ready', establishConsumer);
    }

    await this.closeConsumer(consumer);
  }

  /**
   * Declares exchange and reports 406 error.
   * @param  {Object} params - Exchange params.
   * @returns {Promise<*>}
   */
  declareExchange(params) {
    return this._amqp
      .exchangeAsync(params)
      .call('declareAsync')
      .catch(error406, this._on406.bind(this, params));
  }

  /**
   * Binds exchange to queue via route. For Headers exchange
   * automatically populates arguments with routing-key: <route>.
   * @param  {string} exchange - Exchange to bind to.
   * @param  {Queue} queue - Declared queue object.
   * @param  {string} route - Routing key.
   * @param  {boolean} [headerName=false] - if exchange has `headers` type.
   * @returns {Promise<*>}
   */
  async bindRoute(exchange, queue, route, headerName = false) {
    const queueName = queue.queueOptions.queue;
    const options = {};
    let routingKey;

    if (headerName === false) {
      routingKey = route;
    } else {
      options.arguments = {
        'x-match': 'any',
        [headerName === true ? 'routing-key' : headerName]: route,
      };
      routingKey = '';
    }

    const response = await queue.bindAsync(exchange, routingKey, options);
    const { _routes: routes } = queue;

    if (Array.isArray(routes)) {
      // reconnect might push an extra route
      if (!routes.includes(route)) {
        routes.push(route);
      }

      this.log.trace({ routes, queueName }, '[queue routes]');
    }

    this.log.debug({ queueName, exchange, routingKey }, 'bound queue to exchange');

    return response;
  }

  /**
   * Bind specified queue to exchange
   *
   * @param {object} queue     - queue instance created by .createQueue
   * @param {string} _routes   - messages sent to this route will be delivered to queue
   * @param {object} [opts={}] - exchange parameters:
   *                 https://github.com/dropbox/amqp-coffee#connectionexchangeexchangeargscallback
   */
  bindExchange(queue, _routes, opts = {}) {
    // make sure we have an expanded array of routes
    const routes = toUniqueStringArray(_routes);

    // default params
    const params = merge({
      exchange: this.config.exchange,
      type: this.config.exchangeArgs.type,
      durable: true,
      autoDelete: false,
    }, opts);

    const { exchange } = params;
    assert(exchange, 'exchange name must be specified');
    this.log.debug('bind routes->exchange', routes, exchange);

    return this.declareExchange(params)
      .return(routes)
      .map((route) => (
        this.bindRoute(exchange, queue, route)
      ));
  }

  /**
   * Binds multiple routing keys to headers exchange.
   * @param  {Object} queue
   * @param  {mixed} _routes
   * @param  {Object} opts
   * @param  {boolean} [headerName=false] - if exchange has `headers` type
   * @returns {Promise<*>}
   */
  bindHeadersExchange(queue, _routes, opts, headerName = true) {
    // make sure we have an expanded array of routes
    const routes = toUniqueStringArray(_routes);
    // default params
    const params = merge({ durable: true, autoDelete: false }, opts);
    const { exchange } = params;

    // headers exchange
    // do sanity check
    assert.equal(params.type, 'headers');
    assert.ok(exchange, 'exchange must be set');

    this.log.debug('bind routes->exchange/headers', routes, exchange);

    return this.declareExchange(params)
      .return(routes)
      .map((route) => {
        assert.ok(/^[^*#]+$/.test(route));
        return this.bindRoute(exchange, queue, route, headerName);
      });
  }

  /**
   * Unbind specified queue from exchange
   *
   * @param {object} queue   - queue instance created by .createQueue
   * @param {string} _routes - messages sent to this route will be delivered to queue
   */
  unbindExchange(queue, _routes) {
    const { exchange } = this.config;
    const routes = toUniqueStringArray(_routes);

    return Promise.map(routes, (route) => (
      queue.unbindAsync(exchange, route).tap(() => {
        const queueName = queue.queueOptions.queue;
        if (queue._routes) {
          const idx = queue._routes.indexOf(route);
          if (idx >= 0) {
            queue._routes.splice(idx, 1);
          }

          this.log.debug({ routes: queue._routes }, 'queue routes');
        }

        this.log.info({ queueName, exchange, route }, 'queue unbound from exchange');
      })
    ));
  }

  /**
   * Low-level publishing method
   * @param  {string} exchange
   * @param  {string} queueOrRoute
   * @param  {mixed} _message
   * @param  {Object} options
   * @returns {Promise<*>}
   */
  async sendToServer(exchange, queueOrRoute, _message, options) {
    const publishOptions = this._publishOptions(options);
    const message = options.skipSerialize === true
      ? _message
      : await serialize(_message, publishOptions);

    const { _amqp: amqp } = this;
    if (!amqp) {
      // NOTE: if this happens - it means somebody
      // called (publish|send)* after amqp.close()
      // or there is an auto-retry policy that does the same
      throw new InvalidOperationError('connection was closed');
    }

    const request = await amqp
      .publishAsync(exchange, queueOrRoute, message, publishOptions);

    // emit original message
    this.emit('publish', queueOrRoute, _message);

    return request;
  }

  /**
   * Send message to specified route
   *
   * @param   {String} route   - destination route
   * @param   {mixed}  message - message to send - will be coerced to string via stringify
   * @param   {Object} options - additional options
   * @param   {Span}   parentSpan
   */
  publish(route, message, options = {}, parentSpan) {
    const span = this.tracer.startSpan(`publish:${route}`, {
      childOf: parentSpan,
    });

    // prepare exchange
    const exchange = is.string(options.exchange)
      ? options.exchange
      : this.config.exchange;

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_MESSAGING_PRODUCER,
      [Tags.MESSAGE_BUS_DESTINATION]: `${exchange}:${route}`,
    });

    return wrapPromise(span, this.sendToServer(
      exchange,
      route,
      message,
      options
    ));
  }

  /**
   * Send message to specified queue directly
   *
   * @param {String} queue     - destination queue
   * @param {mixed}  message   - message to send
   * @param {Object} [options] - additional options
   * @param {opentracing.Span} [parentSpan] - Existing span.
   */
  send(queue, message, options = {}, parentSpan) {
    const span = this.tracer.startSpan(`send:${queue}`, {
      childOf: parentSpan,
    });

    // prepare exchange
    const exchange = is.string(options.exchange)
      ? options.exchange
      : '';

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_MESSAGING_PRODUCER,
      [Tags.MESSAGE_BUS_DESTINATION]: `${exchange || '<empty>'}:${queue}`,
    });

    return wrapPromise(span, this.sendToServer(
      exchange,
      queue,
      message,
      options
    ));
  }

  /**
   * Sends a message and then awaits for response
   * @param  {String} route
   * @param  {mixed}  message
   * @param  {Object} options
   * @param  {Span}   parentSpan
   * @return {Promise}
   */
  publishAndWait(route, message, options = {}, parentSpan) {
    // opentracing instrumentation
    const span = this.tracer.startSpan(`publishAndWait:${route}`, {
      childOf: parentSpan,
    });

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_RPC_CLIENT,
      [Tags.MESSAGE_BUS_DESTINATION]: route,
    });

    return wrapPromise(span, this.createMessageHandler(
      route,
      message,
      options,
      this.publish,
      span
    ));
  }

  /**
   * Send message to specified queue directly and wait for answer
   *
   * @param {string} queue        destination queue
   * @param {any}    message      message to send
   * @param {object} options      additional options
   * @param {Span}   parentSpan
   */
  sendAndWait(queue, message, options = {}, parentSpan) {
    // opentracing instrumentation
    const span = this.tracer.startSpan(`sendAndWait:${queue}`, {
      childOf: parentSpan,
    });

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_RPC_CLIENT,
      [Tags.MESSAGE_BUS_DESTINATION]: queue,
    });

    return wrapPromise(span, this.createMessageHandler(
      queue,
      message,
      options,
      this.send
    ));
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
    // remove unused opts
    const { skipSerialize, gzip: needsGzip, ...opts } = options;

    // force contentEncoding
    if (needsGzip === true) {
      opts.contentEncoding = 'gzip';
    }

    // set default opts
    defaults(opts, this._defaultOpts);

    // append request timeout in headers
    defaults(opts.headers, {
      timeout: opts.timeout || this.config.timeout,
    });

    return opts;
  }

  _replyOptions(options = {}) {
    return {
      simpleResponse: options.simpleResponse === undefined
        ? this._defaultOpts.simpleResponse
        : options.simpleResponse,
    };
  }

  /**
   * Reply to sender queue based on headers
   *
   * @param   {Object} properties - incoming message headers
   * @param   {mixed}  message - message to send
   * @param   {Span}   [span] - opentracing span
   * @param   {AMQPMessage} [raw] - raw message
   */
  reply(properties, message, span, raw) {
    if (!properties.replyTo || !properties.correlationId) {
      const error = new ValidationError('replyTo and correlationId not found in properties', 400);

      if (span !== undefined) {
        span.setTag(Tags.ERROR, true);
        span.log({
          event: 'error', 'error.object': error, message: error.message, stack: error.stack,
        });
        span.finish();
      }

      if (raw !== undefined) {
        this.emit('after', raw);
      }

      return Promise.reject(error);
    }

    const options = {
      correlationId: properties.correlationId,
    };

    if (properties[kReplyHeaders]) {
      options.headers = properties[kReplyHeaders];
    }

    let promise = this.send(properties.replyTo, message, options, span);

    if (raw !== undefined) {
      promise = promise
        .finally(() => this.emit('after', raw));
    }

    return span === undefined
      ? promise
      : wrapPromise(span, promise);
  }

  /**
   * Creates local listener for when a private queue is up
   * @returns {Promise<Void|Error>}
   */
  awaitPrivateQueue() {
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
   * @param  {String}   routing
   * @param  {Object}   options
   * @param  {String}   message
   * @param  {Function} publishMessage
   * @param  {Span}     span - opentracing span
   * @return {Promise}
   */
  async createMessageHandler(routing, message, options, publishMessage, span) {
    assert(typeof options === 'object' && options !== null, 'options must be an object');

    const replyTo = options.replyTo || this._replyTo;
    const time = process.hrtime();
    const replyOptions = this._replyOptions(options);

    // ensure that reply queue exists before sending request
    if (typeof replyTo !== 'string') {
      if (replyTo === false) {
        await this.awaitPrivateQueue();
      } else {
        await this.createPrivateQueue();
      }

      return this.createMessageHandler(routing, message, options, publishMessage, span);
    }

    // work with cache if options.cache is set and is number
    // otherwise cachedResponse is always null
    const cachedResponse = this.cache.get(message, options.cache);
    if (cachedResponse !== null && typeof cachedResponse === 'object') {
      return adaptResponse(cachedResponse.value, replyOptions);
    }

    const { replyStorage } = this;
    // generate response id
    const correlationId = options.correlationId || uuid.v4();
    // timeout before RPC times out
    const timeout = options.timeout || this.config.timeout;

    // slightly longer timeout, if message was not consumed in time, it will return with expiration
    const publishPromise = new Promise((resolve, reject) => {
      // push into RPC request storage
      replyStorage.push(correlationId, {
        timeout,
        time,
        routing,
        resolve,
        reject,
        replyOptions,
        cache: cachedResponse,
        timer: null,
      });
    });

    // debugging
    this.log.trace('message pushed into reply queue in %s', latency(time));

    // add custom header for routing over amq.headers exchange
    if (!options.headers) {
      options.headers = Object.create(null);
    }
    options.headers['reply-to'] = replyTo;

    // add opentracing instrumentation
    if (span) {
      this.tracer.inject(span.context(), FORMAT_TEXT_MAP, options.headers);
    }

    // this is to ensure that queue is not overflown and work will not
    // be completed later on
    publishMessage
      .call(this, routing, message, {
        ...options,
        replyTo,
        correlationId,
        expiration: Math.ceil(timeout * 0.9).toString(),
      }, span)
      .tap(() => {
        this.log.trace({ latency: latency(time) }, 'message published');
      })
      .catch((err) => {
        this.log.error({ err }, 'error sending message');
        replyStorage.reject(correlationId, err);
      });

    return publishPromise;
  }

  /**
   *
   * @param  {Object} message
   *  - @param {Object} data: a getter that returns the data in its parsed form, eg a
   *                           parsed json object, a string, or the raw buffer
   *  - @param {Object} raw: the raw buffer that was returned
   *  - @param {Object} properties: headers specified for the message
   *  - @param {Number} size: message body size
   *  - @param {Function} ack(): function : only used when prefetchCount is specified
   *  - @param {Function} reject(): function: only used when prefetchCount is specified
   *  - @param {Function} retry(): function: only used when prefetchCount is specified
   */
  _onConsume(_router) {
    assert(is.fn(_router), '`router` must be a function');

    // use bind as it is now fast
    const amqpTransport = this;
    const parseInput = amqpTransport._parseInput.bind(amqpTransport);
    const router = _router.bind(amqpTransport);

    return async function consumeMessage(incoming) {
      // emit pre processing hook
      amqpTransport.emit('pre', incoming);

      // extract message data
      const { properties } = incoming;
      const { contentType, contentEncoding } = properties;

      // parsed input data
      const message = await parseInput(incoming.raw, contentType, contentEncoding);
      // useful message properties
      const props = { ...properties, ...pick(incoming, extendMessageProperties) };

      // pass to the consumer message router
      // message - properties - incoming
      //  incoming.raw<{ ack: ?Function, reject: ?Function, retry: ?Function }>
      //  and everything else from amqp-coffee
      setImmediate(router, message, props, incoming);
    };
  }

  /**
   * Distributes messages from a private queue
   * @param  {mixed}  message
   * @param  {Object} properties
   */
  _privateMessageRouter(message, properties/* , raw */) { // if private queue has nack set - we must ack msg
    const { correlationId, replyTo, headers } = properties;
    const { 'x-death': xDeath } = headers;

    // retrieve promised message
    const future = this.replyStorage.pop(correlationId);

    // case 1 - for some reason there is no saved reference, example - crashed process
    if (future === undefined) {
      this.log.error('no recipient for the message %j and id %s', message.error || message.data || message, correlationId);

      let error;
      if (xDeath) {
        error = new AmqpDLXError(xDeath, message);
        this.log.warn('message was not processed', error);
      }

      // otherwise we just run messages in circles
      if (replyTo && replyTo !== this._replyTo) {
        // if error is undefined - generate this
        if (error === undefined) {
          error = new NotPermittedError(`no recipients found for correlationId "${correlationId}"`);
        }

        // reply with the error
        return this.reply(properties, { error });
      }

      // we are done
      return null;
    }

    this.log.trace('response returned in %s', latency(future.time));

    // if message was dead-lettered - reject with an error
    if (xDeath) {
      return future.reject(new AmqpDLXError(xDeath, message));
    }

    if (message.error) {
      const error = wrapError(message.error);

      Object.defineProperty(error, kReplyHeaders, {
        value: headers,
        enumerable: false,
      });

      return future.reject(error);
    }

    const response = buildResponse(message, properties);
    this.cache.set(future.cache, response);

    return future.resolve(adaptResponse(response, future.replyOptions));
  }

  async _parseInput(_data, contentType = 'application/json', contentEncoding = 'plain') {
    let data;

    switch (contentEncoding) {
      case 'gzip':
        data = await gunzip(_data).catchReturn({ err: PARSE_ERR });
        break;

      case 'plain':
        data = _data;
        break;

      default:
        return { err: PARSE_ERR };
    }

    switch (contentType) {
      // default encoding when we were pre-stringifying and sending str
      // and our updated encoding when we send buffer now
      case 'string/utf8':
      case 'application/json':
        return safeJSONParse(data, this.log);

      default:
        return data;
    }
  }

  /**
   * Handle 406 Error.
   * @param  {Object} params - exchange params
   * @param  {Error}  err    - 406 Conflict Error.
   */
  _on406(params, err) {
    this.log.warn({ params }, '[406] error declaring exchange/queue:', err.replyText);
  }

  /**
   * 'ready' event from amqp-coffee lib, perform queue recreation here
   */
  _onConnect() {
    const { serverProperties } = this._amqp;
    const { cluster_name: clusterName, version } = serverProperties;

    // emit connect event through log
    this.log.info('connected to %s v%s', clusterName, version);

    // https://github.com/dropbox/amqp-coffee#reconnect-flow
    // recreate unnamed private queue
    if ((this._replyTo || this.config.private) && this._replyTo !== false) {
      this.createPrivateQueue();
    }

    // re-emit ready
    this.emit('ready');
  }

  /**
   * Pass in close event
   */
  _onClose(err) {
    // emit connect event through log
    this.log.warn({ err }, 'connection is closed');
    // re-emit close event
    this.emit('close', err);
  }
}

// expose static connectors
helpers(AMQPTransport);

// assign statics
module.exports = AMQPTransport;
