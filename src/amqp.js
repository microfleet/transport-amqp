/**
 * @typedef { import("opentracing").Span } Span
 * @typedef { import("opentracing").Tracer } Tracer
 *
 * @typedef AMQPMessage
 * @property {Object} properties - amqp properties
 * @property {Record<string, any>} properties.headers - amqp headers
 * @property {string} [properties.appId] sender diagnostics data
 * @property {string} [properties.routingKey] amqp routing-key
 * @property {string} [properties.replyTo] amqp reply-to field
 * @property {string} [properties.correlationId] amqp correlation-id
 * @property {string} [properties.contentType] amqp message content-type
 * @property {string} [properties.contentEncoding] amqp message content-encoding
 * @property {() => void} [ack] - Acknowledge if nack is `true`.
 * @property {() => void} [reject] - Reject if nack is `true`.
 * @property {() => void} [retry] - Retry msg if nack is `true`.
 * @property {opentracing.Span} [span] - optional span
 *
 * @typedef ConsumerBase
 * @property {(err: any, result?: any) => void} cancel
 * @property {() => void} close
 * @property {string} consumerTag
 *
 * @typedef {EventEmitter & ConsumerBase} Consumer
 *
 * @typedef PublishOptions
 * @property {string} [contentType] - message content-type
 * @property {string} [contentEncoding] - message encoding, defaults to `plain`
 * @property {string} [exchange] - exchange to publish to
 * @property {string} [correlationId] - used to match messages when getting a reply
 * @property {string} [exchange] - will be overwritten by exchange thats passed
 * @property {boolean} [gzip] - whether to encode using gzip
 * @property {boolean} [skipSerialize] - whether it was already serialized earlier
 * @property {number}  [timeout] - optional ttl value for message in the publish/send methods
 *                              https://github.com/dropbox/amqp-coffee/blob/6d99cf4c9e312c9e5856897ab33458afbdd214e5/src/lib/Publisher.coffee#L90
 *
 * @property {Record<string, string | number | boolean>} [headers]
 *
 * @typedef QueueBindOptions
 * @property {boolean} [autoDelete]
 * @property {Record<string, string>} [arguments]
 *
 * @typedef Queue
 * @property {Object} queueOptions
 * @property {string} queueOptions.queue
 * @property {string[]} [_routes]
 * @property {(exchange: string, routingKey: string, options: QueueBindOptions) => Promise<any>} bindAsync
 *
 * @typedef AMQPError
 * @property {string} replyText
 */

// deps
const Bluebird = require('bluebird');
const gunzip = Bluebird.promisify(require('zlib').gunzip);
const gzip = Bluebird.promisify(require('zlib').gzip);
const uuid = require('uuid');
const flatstr = require('flatstr');
const stringify = require('json-stringify-safe');
const EventEmitter = require('eventemitter3');
const { once } = require('events');
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
  HttpStatusError,
} = require('common-errors');

// lodash fp
const merge = require('lodash/merge');
const defaults = require('lodash/defaults');
const noop = require('lodash/noop');
const uniq = require('lodash/uniq');
const pick = require('lodash/pick');
const readPkg = require('read-pkg');

// local deps
const { Joi, schema } = require('./schema');
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
const PARSE_ERR = new ValidationError('couldn\'t deserialize input', '500', 'message.raw');
const pkg = readPkg.sync();

/**
 * Wraps regular in a bluebird promise
 * @template T
 * @param  {Span} span opentracing span
 * @param  {PromiseLike<T>} promise pending promise
 * @return {Bluebird<T>}
 */
const wrapPromise = (span, promise) => Bluebird.resolve((async () => {
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

/**
 *
 * @param {any} message
 * @param {PublishOptions} publishOptions
 * @returns
 */
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

/**
 * @param {any} data
 * @param {import("pino").BaseLogger} log
 * @returns
 */
function safeJSONParse(data, log) {
  try {
    return JSON.parse(data, jsonDeserializer);
  } catch (err) {
    log.warn({ err, data: String(data) }, 'Error parsing buffer');
    return { err: PARSE_ERR };
  }
}

/**
 * @template {string} T
 * @param {T | T[]} routes
 * @returns {T[]}
 */
const toUniqueStringArray = (routes) => (
  Array.isArray(routes) ? uniq(routes) : [routes]
);

/**
 * Routing function HOC with reply RPC enhancer
 * @param  {Function} messageHandler
 * @param  {AMQPTransport} transport
 * @returns {(message: any, properties: AMQPMessage['properties'], raw: AMQPMessage) => any}
 */
const initRoutingFn = (messageHandler, transport) => {
  /**
   * Response Handler Function. Sends Reply or Noop log.
   * @param  {AMQPMessage} raw - Raw AMQP Message Structure
   * @param  {Error | null} error - Error if it happened.
   * @param  {any} data - Response data.
   * @returns {Promise<any>}
   */
  async function responseHandler(raw, error, data) {
    const { properties, span } = raw;
    return !properties.replyTo || !properties.correlationId
      ? transport.noop(error, data, span, raw)
      : transport.reply(properties, { error, data }, span, raw);
  }

  /**
   * Initiates consumer message handler.
   * @param  {any} message - Data passed from the publisher.
   * @param  {AMQPMessage['properties']} properties - AMQP Message properties.
   * @param  {AMQPMessage} raw - Original AMQP message.
   * @returns {any}
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
 * @template [T=any]
 * @param {Object} response
 * @oaram {T} response.data
 * @oaram {Record<string, any>} response.headers
 * @param {Object} replyOptions
 * @param {boolean} replyOptions.simpleResponse
 * @returns {T | { data: T, headers: Record<string, any>}}
 */
function adaptResponse(response, replyOptions) {
  return replyOptions.simpleResponse === false ? response : response.data;
}

/**
 * @template [T=any]
 * @param {Object} message
 * @param {T} message.data
 * @param {Error | null} message.error
 * @param {AMQPMessage['properties']} properties
 * @returns {{ data: T, headers: AMQPMessage['properties']['headers'] }}
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

    /**
     * @readonly
     * reply storage, where we'd save correlation ids
     * and callbacks to be called once we are done
     */
    this.replyStorage = new ReplyStorage();

    /**
     * delay settings for reconnect
     * @readonly
     */
    this.recovery = new Backoff(config.recovery);

    /**
     * @readonly
     * @type Tracer
     */
    this.tracer = config.tracer || new opentracing.Tracer();

    /**
     * @private
     */
    this._replyTo = null;
    /**
     * @private
     */
    this._consumers = new Map();
    /**
     * @private
     */
    this._queues = new WeakMap();
    /**
     * @private
     */
    this._reconnectionHandlers = new WeakMap();
    /**
     * @private
     */
    this._boundEmit = this.emit.bind(this);
    /**
     * @private
     */
    this._onConsume = this._onConsume.bind(this);
    /**
     * @private
     */
    this._on406 = this._on406.bind(this);
    /**
     * @private
     */
    this._onClose = this._onClose.bind(this);
    /**
     * @private
     */
    this._onConnect = this._onConnect.bind(this);

    // Form app id string for debugging
    /**
     * @private
     */
    this._appID = {
      name: this.config.name,
      host: os.hostname(),
      pid: process.pid,
      utils_version: pkg.version,
      version: opts.version || 'n/a',
    };

    // Cached serialized value
    /**
     * @private
     */
    this._appIDString = stringify(this._appID);
    /**
     * @private
     */
    this._defaultOpts = { ...config.defaultOpts };
    this._defaultOpts.appId = this._appIDString;
    /**
     * @private
     */
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
   * @return {Bluebird<AMQPTransport>}
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
          return Bluebird.reject(err);
        }

        default:
          // already closed, but make sure
          amqp.close();
          this._amqp = null;
      }
    }

    return Bluebird
      .fromNode((next) => {
        this._amqp = new AMQP(config.connection, next);
        this._amqp.on('ready', this._onConnect);
        this._amqp.on('close', this._onClose);
      })
      .return(this);
  }

  /**
   * Noop function with empty correlation id and reply to data
   * @param  {Error | null} error
   * @param  {any} data
   * @param  {Span} [span]
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
      await new Bluebird((resolve, reject) => {
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
          return Bluebird.resolve();
      }
    }

    const err = new InvalidOperationError('connection was not initialized in the first place');
    return Bluebird.reject(err);
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
    const userParams = typeof opts === 'string' ? { queue: opts } : opts;
    const requestedName = userParams.queue;
    const params = merge({ autoDelete: !requestedName, durable: !!requestedName }, userParams);

    log.debug({ params }, 'initializing queue');

    const queue = ctx.queue = await amqp.queueAsync(params);

    await Bluebird
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
      this.log.error({ err: error }, 'consumer returned 404 error');

      // reset replyTo queue and ignore all future errors
      consumer.removeAllListeners('error');
      consumer.removeAllListeners('cancel');
      consumer.on('error', noop);
      consumer.close();

      // recreate queue
      if (this._replyTo !== false) this.createPrivateQueue();

      return;
    }

    this.log.error({ err }, 'private consumer returned err');
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
  /**
   *
   * @param {Consumer} consumer
   * @param {Queue} queue
   * @param {any} err
   * @param {any} res
   * @returns
   */
  async handleConsumerError(consumer, queue, err, res) {
    const error = err.error || err;

    // https://www.rabbitmq.com/amqp-0-9-1-reference.html -
    switch (error.replyCode) {
      // ignore errors
      case 311:
      case 313:
        this.log.error({ err, res }, 'error working with a channel');
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

  /**
   *
   * @param {Consumer} consumer
   * @param {AMQPError | null} err
   * @param {any} res
   * @returns
   */
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
      await Bluebird.delay(this.recovery.get('consumed', 1));
    } catch (e) {
      this.log.error({ err: e }, 'failed to close consumer');
    } finally {
      await establishConsumer();
    }
  }

  /**
   * @param {Consumer} consumer
   */
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
   * @param {number} [attempt]
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
      this.log.error({ err: e }, 'private queue creation failed - restarting');
      await Bluebird.delay(this.recovery.get('private', attempt));
      return this.createPrivateQueue(attempt + 1);
    }

    this.log.debug({ queue: this._replyTo }, 'private-queue-ready');
    setImmediate(this._boundEmit, 'private-queue-ready');

    return createdQueue;
  }

  /**
   *
   * @param {string[]} routes
   * @param {*} queue
   * @param {*} oldQueue
   * @returns
   */
  async bindQueueToExchangeOnRoutes(routes, queue, oldQueue = Object.create(null)) {
    /**
     * @type {string[]}
     */
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

    await Bluebird.all(work);
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
    const baseOpts = {
      router,
      neck: config.neck,
      noAck: config.noAck,
      queue: config.queue || '',
    };

    const queueOptions = merge(
      baseOpts,
      config.defaultQueueOpts,
      this._extraQueueOptions,
      options
    );

    if (config.bindPersistantQueueToHeadersExchange === true) {
      for (const route of listen.values()) {
        assert.ok(
          /^[^*#]+$/.test(route),
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
        await Bluebird.delay(recovery.get('consumed', attempt + 1));
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
    await Bluebird.all(work);
  }

  /**
   * Utility function to close consumer and forget about it
   * @param {Consumer} consumer
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
    await Bluebird
      .fromCallback((done) => {
        consumer.cancel(done);
      })
      .timeout(5000)
      .catch(Bluebird.TimeoutError, noop);

    this.log.info({ consumerTag: consumer.consumerTag }, 'closed consumer');
  }

  /**
   * Prevents consumer from re-establishing connection
   * @param {Consumer} [consumer]
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
   * @returns {Bluebird<any>}
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
   * @param  {string | boolean} [headerName=false] - if exchange has `headers` type.
   * @returns {Promise<any>}
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
   * @param {string | string[]} _routes   - messages sent to this route will be delivered to queue
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
   * @param  {string | string[]} _routes
   * @param  {Object} opts
   * @param  {string | boolean} [headerName=true] - if exchange has `headers` type
   * @returns {Bluebird<*>}
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
   * @param {string | string[]} _routes - messages sent to this route will be delivered to queue
   * @returns {Bluebird<any>}
   */
  unbindExchange(queue, _routes) {
    const { exchange } = this.config;
    const routes = toUniqueStringArray(_routes);

    return Bluebird.map(routes, (route) => (
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
   * @param  {any} _message
   * @param  {PublishOptions} options
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
   * @template [T=any]
   *
   * @param {String} route - Destination route
   * @param {any} message - Message to send - will be coerced to string via stringify
   * @param {PublishOptions} [options={}] - Additional options
   * @param {opentracing.Span} [parentSpan] - Existing span
   * @returns {Bluebird<T>}
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
   * @template [T=any]
   *
   * @param {String} queue - Destination queue
   * @param {any} message - Message to send
   * @param {PublishOptions} [options={}] - Additional options
   * @param {opentracing.Span} [parentSpan] - Existing span
   * @returns {Bluebird<T>}
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
   *
   * @template [T=any]
   *
   * @param {String} route - Destination route
   * @param {any} message - Message to send - will be coerced to string via stringify
   * @param {PublishOptions} [options={}] - Additional options
   * @param {opentracing.Span} [parentSpan] - Existing span
   * @returns {Promise<T>}
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
   * @template [T=any]
   *
   * @param {string} queue - Destination queue
   * @param {any} message - Message to send
   * @param {PublishOptions} [options={}] - Additional options
   * @param {opentracing.Span} [parentSpan] - Existing span
   * @returns {Bluebird<T>}
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
   * @param {PublishOptions} options
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
   * @param   {any}  message - message to send
   * @param   {Span}   [span] - opentracing span
   * @param   {AMQPMessage} [raw] - raw message
   * @returns {Bluebird<any>}
   */
  reply(properties, message, span, raw) {
    if (!properties.replyTo || !properties.correlationId) {
      const error = new HttpStatusError(400, 'replyTo and correlationId not found in properties');

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

      return Bluebird.reject(error);
    }

    /**
     * @type {PublishOptions}
     */
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
   * @returns {Promise<void>}
   */
  async awaitPrivateQueue() {
    await once(this, 'private-queue-ready');
  }

  /**
   * Creates response message handler and sets timeout on the response
   * @param  {String}   routing
   * @param  {Object}   options
   * @param  {String}   message
   * @param  {Function} publishMessage
   * @param  {Span}     [span] - opentracing span
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
   */
  _onConsume(_router) {
    assert(is.fn(_router), '`router` must be a function');

    // use bind as it is now fast
    const amqpTransport = this;
    const parseInput = amqpTransport._parseInput.bind(amqpTransport);
    const router = _router.bind(amqpTransport);

    /**
     * @param  {Object} incoming
     * @param {Object} incoming.data: a getter that returns the data in its parsed form, eg a
     *                           parsed json object, a string, or the raw buffer
     * @param {AMQPMessage['properties']} incoming.properties
     * @param {AMQPMessage} incoming.raw
     */
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
   * @param  {any}  message
   * @param  {AMQPMessage['properties']} properties
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
        this.log.warn({ err: error }, 'message was not processed');
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

  /**
   * Parses AMQP message
   * @template [T=any]
   * @param  {Buffer} _data
   * @param  {String} [contentType='application/json']
   * @param  {String} [contentEncoding='plain']
   * @return {Promise<T | { err: Error }>}
   */
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
   * @param  {AMQPError}  err    - 406 Conflict Error.
   */
  _on406(params, err) {
    this.log.warn({ params }, '[406] error declaring exchange/queue: %s', err.replyText);
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

/**
 * Creates AMQPTransport instance
 * @template {Function} T
 * @param  {Object} [_config]
 * @param  {T} [_messageHandler]
 * @returns {Bluebird<[AMQPTransport, T]>}
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
 * @param  {Record<string, any>} config
 * @param  {Function} [_messageHandler]
 * @param  {Object} [_opts={}]
 * @returns {Bluebird<AMQPTransport>}
 */
AMQPTransport.connect = function connect(config, _messageHandler, _opts = {}) {
  return AMQPTransport
    .create(config, _messageHandler)
    .then(async ([amqp, messageHandler]) => {
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
 * @param  {Object} [opts={}]
 * @returns {Bluebird<AMQPTransport>}
 */
AMQPTransport.multiConnect = function multiConnect(config, _messageHandler, opts = []) {
  return AMQPTransport
    .create(config, _messageHandler)
    .then(async ([amqp, messageHandler]) => {
      // do not init queues
      if (is.fn(messageHandler) === false && !amqp.config.listen) {
        return amqp;
      }

      await Bluebird.map(amqp.config.listen, (route, idx) => {
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

// expose internal libraries
AMQPTransport.ReplyStorage = ReplyStorage;
AMQPTransport.Backoff = Backoff;
AMQPTransport.Cache = Cache;
AMQPTransport.jsonSerializer = jsonSerializer;
AMQPTransport.jsonDeserializer = jsonDeserializer;

// assign statics
module.exports = AMQPTransport;
