import is from '@sindresorhus/is'
import assert from 'assert'

import { ArgumentError, ConnectionError, InvalidOperationError, NotPermittedError, ValidationError } from 'common-errors'
import delay from 'delay'
import EventEmitter from 'eventemitter3'
import defaults from 'lodash/defaults'
import merge from 'lodash/merge'
import opentracing, { FORMAT_TEXT_MAP, Tags } from 'opentracing'
import stringify from 'safe-stable-stringify'

import { kReplyHeaders } from './constants'
import {
  Consumer,
  ConsumerOpts,
  ConsumerFactory,
  PrivateConsumer
} from './consumer'
import { MessageOptions, PublishOptions } from './message-options'
import { AnyExchangeOpts, ExchangeFactory } from './exchange'
import { getInstance as getLoggerInstance } from './loggers'

import { Queue, QueueConfOpts, QueueFactory, QueueOpts } from './queue'
import { PendingReplyConf, ReplyStorage } from './reply-storage'

import { Schema } from './schema'
import { BackoffPolicy } from './schema/backoff'
import { SequenceProvider } from './sequence-provider'
import {
  WellKnowHeaders,
  AMQPTransportEvents,
  PublishMessageHandle,
  RawMessage,
} from './types'

import type { LoggerLike } from './schema/logger-like'
import type {
  AnyFunction,
  ConsumedQueue,
  MessageHandler,
} from './types'

import { Backoff } from './backoff'
import { Cache } from './response-cache'
import { AppID, getAppID } from './utils/get-app-id'
import { initRoutingFn } from './utils/init-routing-fn'
import { Message, buildResponse, adaptResponse} from './utils/response'

// TODO
import type { AMQP } from './utils/transport'
// TODO
import Transport from './utils/transport'
import { latency } from './utils/latency'
import { AmqpDLXError, wrapError } from './utils/error'
import { jsonSerializer, serialize } from './utils/serialization'
import { wrapPromise } from './utils/wrap-promise'

export class AMQPTransport extends EventEmitter {
  public config: Schema
  public log: LoggerLike

  readonly #appIDString: string
  readonly #appID: AppID
  readonly #defaultOpts: Partial<PublishOptions>
  readonly #extraQueueOpts: Partial<PublishOptions> = {}
  readonly #sequenceProvider: SequenceProvider

  #replyTo?: string | false
  #cache: Cache
  #tracer: opentracing.Tracer
  #recovery: Backoff
  #replyStorage: ReplyStorage

  #amqp: AMQP.Instance | null = null
  #queues: QueueFactory
  #consumers: ConsumerFactory
  #exchangeFactory: ExchangeFactory
  #reconnectionHandlers: WeakMap<Consumer, AnyFunction> = new WeakMap<Consumer, AnyFunction>()

  public static async create<RequestBody, ResponseBody>(
    config: MessageHandler<RequestBody, ResponseBody>
  ): Promise<[AMQPTransport, MessageHandler<RequestBody, ResponseBody>]>
  public static async create<RequestBody, ResponseBody>(
    config: Partial<Schema>,
    messageHandler: MessageHandler<RequestBody, ResponseBody>
  ): Promise<[AMQPTransport, MessageHandler<RequestBody, ResponseBody>]>
  public static async create<RequestBody, ResponseBody>(
    config: any,
    messageHandler?: MessageHandler<RequestBody, ResponseBody>
  ): Promise<[AMQPTransport, MessageHandler<RequestBody, ResponseBody> | undefined]> {
    let $config = config
    let $messageHandler = messageHandler

    if (typeof config === 'function' && is.undefined(messageHandler)) {
      $messageHandler = config
      $config = {}
    }

    const amqp = new AMQPTransport($config)
    await amqp.connect()

    return [amqp, $messageHandler]
  }

  public static async connect<RequestBody, ResponseBody>(
    config: Partial<Schema>,
    messageHandler: MessageHandler<RequestBody, ResponseBody>,
    opts: Partial<PublishOptions> = {}
  ): Promise<AMQPTransport> {
    const [
      amqp,
      $messageHandler
    ] = await AMQPTransport.create(config, messageHandler)

    if (typeof $messageHandler === 'function' || amqp.config.listen) {
      await amqp.createConsumedQueue(
        $messageHandler,
        amqp.config.listen,
        opts
      )
    }

    return amqp
  }

  public static async multiConnect<RequestBody, ResponseBody>(
    config: Partial<Schema>,
    messageHandler: MessageHandler<RequestBody, ResponseBody>,
    opts: Partial<PublishOptions> = {}
  ): Promise<AMQPTransport> {
    const [
      amqp,
      $messageHandler
    ] = await AMQPTransport.create(config, messageHandler)

    if (typeof $messageHandler !== 'function' && !amqp.config.listen) {
      return amqp
    }


    await Promise.all(amqp.config.listen.map((route, idx) => {
      // TODO
      // @ts-expect-error
      const queueOpts = opts[idx] ?? Object.create(null)
      const queueName = config.queue
        ? `${config.queue}-${route.replace(/[#*]/g, '.')}`
        : config.queue

      const consumedQueueOpts = defaults(queueOpts, {
        queue: queueName,
      })

      return amqp.createConsumedQueue(messageHandler, [route], consumedQueueOpts)
    }))

    return amqp
  }

  get amqp() { return this.#amqp }
  get appID() { return this.#appID }
  get appIDString() { return this.#appIDString }
  get isConnected() { return this.amqp?.state === 'open' }
  get isConnecting() { return this.#replyTo === false }
  get tracer() { return this.#tracer }

  /**
   * Instantiate AMQP Transport
   * @param  {Object} opts, defaults to {}
   */
  constructor(opts: Partial<Schema> = {}) {
    super()

    const config = this.config = Schema.parse(opts)
    this.#appID = getAppID(this)
    this.#appIDString = stringify(this.#appID)

    // init logger
    this.log = getLoggerInstance(config)
    this.log.debug({ config }, 'used configuration')

    // init cache
    this.#cache = new Cache({
      size: this.config.cache,
      log: this.log,
    })

    // reply storage, where we'd save correlation ids
    // and callbacks to be called once we are done
    this.#replyStorage = new ReplyStorage()

    // delay settings for reconnect
    this.#recovery = new Backoff(config.recovery)

    // init open tracer - default one is noop
    this.#tracer = this.config.tracer ?? new opentracing.Tracer()

    this.#defaultOpts = {
      ...config.defaultOpts,
      appId: this.appIDString,
    }

    this.#sequenceProvider = new SequenceProvider()

    // DLX config
    if (config.dlx.enabled) {
      // there is a quirk - we must make sure that no routing key matches queue name
      // to avoid useless redistributions of the message
      this.#extraQueueOpts.arguments = { 'x-dead-letter-exchange': config.dlx.params.exchange }
    }
  }

  /**
   * Connects to AMQP, if config.router is specified earlier,
   * automatically invokes .consume function
   * @return {Promise}
   */
  async connect() {
    const { amqp, config } = this

    if (amqp) {
      switch (amqp.state) {
        case 'opening':
        case 'open':
        case 'reconnecting': {
          const msg = 'connection was already initialized, close it first'
          throw new InvalidOperationError(msg)
        }

        default:
          // already closed, but make sure
          amqp.close()
          this.#amqp = null
      }
    }

    await new Promise((resolve, reject) => {
      this.#amqp = new Transport(config.connection, (err: any, ...args: any[]) => {
        if (err) {
          return reject(err)
        }

        resolve(args)
      })

      this.#amqp.on('ready', this.#onConnect)
      this.#amqp.on('close', this.#onClose)
    })

    return this
  }

  async close() {
    const amqp = this.#amqp

    if (amqp === null) {
      throw new InvalidOperationError('connection was not initialized in the first place')
    }

    switch (amqp.state) {
      case 'opening':
      case 'open':
      case 'reconnecting':
        return this.#close()

      default:
        this.#amqp = null
        return
    }
  }

  /**
   * Send message to specified route
   *
   * @param   {String} route   - destination route
   * @param   {mixed}  message - message to send - will be coerced to string via stringify
   * @param   {Object} options - additional options
   * @param   {Span}   parentSpan
   */
  async publish<RequestBody extends any>(
    route: string,
    message: RequestBody,
    options: Partial<PublishOptions> = {},
    parentSpan?: opentracing.Span | opentracing.SpanContext
  ) {
    const span = this.tracer.startSpan(`publish:${route}`, {
      childOf: parentSpan,
    })

    // prepare exchange
    const exchange = is.string(options.exchange)
      ? options.exchange
      : this.config.exchange

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_MESSAGING_PRODUCER,
      [Tags.MESSAGE_BUS_DESTINATION]: `${exchange}:${route}`,
    })

    return wrapPromise(span, this.sendToServer(
      exchange,
      route,
      message,
      options
    ))
  }

  /**
   * Sends a message and then awaits for response
   * @param  {String} route
   * @param  {mixed}  message
   * @param  {Object} options
   * @param  {Span}   parentSpan
   * @return {Promise}
   */
  async publishAndWait<
    RequestBody extends any,
    ResponseBody extends any
  > (
    route: string,
    message: RequestBody,
    options: Partial<PublishOptions> = {},
    parentSpan?: opentracing.Span | opentracing.SpanContext
  ) : Promise<ResponseBody> {
    // opentracing instrumentation
    const span = this.tracer.startSpan(`publishAndWait:${route}`, {
      childOf: parentSpan,
    })

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_RPC_CLIENT,
      [Tags.MESSAGE_BUS_DESTINATION]: route,
    })

    return wrapPromise(span, this.createMessageHandler(
      route,
      message,
      options,
      this.publish,
      span
    ))
  }

  /**
   * Send message to specified queue directly
   *
   * @param {String} queue     - destination queue
   * @param {mixed}  message   - message to send
   * @param {Object} [options] - additional options
   * @param {opentracing.Span} [parentSpan] - Existing span.
   */
  async send<RequestBody extends any>(
    queue: string,
    message: RequestBody,
    options: Partial<PublishOptions> = {},
    parentSpan?: opentracing.Span | opentracing.SpanContext
  ) {
    const span = this.tracer.startSpan(`send:${queue}`, {
      childOf: parentSpan,
    })

    // prepare exchange
    const exchange = is.string(options.exchange)
      ? options.exchange
      : ''

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_MESSAGING_PRODUCER,
      [Tags.MESSAGE_BUS_DESTINATION]: `${exchange || '<empty>'}:${queue}`,
    })

    return wrapPromise(span, this.sendToServer(
      exchange,
      queue,
      message,
      options
    ))
  }

  /**
   * Send message to specified queue directly and wait for answer
   *
   * @param {string} queue        destination queue
   * @param {any}    message      message to send
   * @param {object} options      additional options
   * @param {Span}   parentSpan
   */
  async sendAndWait<RequestBody extends any>(
    queue: string,
    message: RequestBody,
    options: Partial<PublishOptions> = {},
    parentSpan?: opentracing.Span | opentracing.SpanContext
  ) {
    // opentracing instrumentation
    const span = this.tracer.startSpan(`sendAndWait:${queue}`, {
      childOf: parentSpan,
    })

    span.addTags({
      [Tags.SPAN_KIND]: Tags.SPAN_KIND_RPC_CLIENT,
      [Tags.MESSAGE_BUS_DESTINATION]: queue,
    })

    return wrapPromise(span, this.createMessageHandler(
      queue,
      message,
      options,
      this.send
    ))
  }

  /**
   * Low-level publishing method
   * @param  {string} exchange
   * @param  {string} queueOrRoute
   * @param  {mixed} _message
   * @param  {Object} options
   * @returns {Promise<*>}
   */
  async sendToServer<RequestBody>(
    exchange: string,
    queueOrRoute: string,
    $message: RequestBody,
    options: Partial<PublishOptions>
  ) {
    const publishOptions = MessageOptions.getPublishOptions(
      options,
      this.#defaultOpts,
      this.config.timeout
    )

    const message = options.skipSerialize === true
      ? $message
      : await serialize($message, publishOptions)

    if (!this.#amqp) {
      // NOTE: if this happens - it means somebody
      // called (publish|send)* after amqp.close()
      // or there is an auto-retry policy that does the same
      throw new InvalidOperationError('connection was closed')
    }

    const request = await this.#amqp
      .publishAsync(exchange, queueOrRoute, message, publishOptions)

    // emit original message
    this.emit(AMQPTransportEvents.Publish, queueOrRoute, $message)

    return request
  }

  async createMessageHandler<
    RequestBody,
    ResponseBody
  >(
    routing: string,
    message: RequestBody,
    options: Partial<PublishOptions>,
    publishMessage: PublishMessageHandle<RequestBody>,
    parentSpan?: opentracing.Span
  ): Promise<ResponseBody> {
    assert(is.plainObject(options), 'options must be an object')

    const replyTo = options.replyTo ?? this.#replyTo
    const time = process.hrtime()
    const replyOptions = MessageOptions.getReplyOptions(
      options,
      this.#defaultOpts
    )

    // ensure that reply queue exists before sending request
    if (typeof replyTo !== 'string') {
      if (replyTo === false) {
        await this.#awaitPrivateQueue()
      } else {
        await this.#createPrivateQueue()
      }

      return this.createMessageHandler(
        routing,
        message,
        options,
        publishMessage,
        parentSpan
      )
    }

    // work with cache if options.cache is set and is number
    // otherwise cachedResponse is always null
    const cachedResponse = this.#cache.get<ResponseBody>(message, options.cache)

    if (is.object(cachedResponse)) {
      return adaptResponse(cachedResponse.value, replyOptions)
    }

    // generate response id
    const correlationId = options.correlationId ?? this.#sequenceProvider.next()
    // timeout before RPC times out
    const timeout = options.timeout ?? this.config.timeout

    // slightly longer timeout, if message was not consumed in time, it will return with expiration
    const publishPromise = new Promise<ResponseBody>((resolve, reject) => {
      const reply: PendingReplyConf & { timer: null } = {
        time,
        timeout,
        routing,
        reject,
        resolve,
        replyOptions,
        cache: cachedResponse,
        timer: null,
      }

      // push into RPC request storage
      this.#replyStorage.push(correlationId, reply)
    })

    // debugging
    this.log.trace('message pushed into reply queue in %s', latency(time))

    // add custom header for routing over amq.headers exchange
    if (!options.headers) {
      options.headers = {
        [WellKnowHeaders.ReplyTo]: replyTo
      }
    } else {
      options.headers[WellKnowHeaders.ReplyTo] = replyTo
    }

    // We don't make a copy of the object to keep it's shape
    options.replyTo = replyTo
    options.correlationId = correlationId
    options.expiration = Math.ceil(timeout * 0.9).toString()

    // add opentracing instrumentation
    if (parentSpan) {
      this.#tracer.inject(
        parentSpan.context(),
        FORMAT_TEXT_MAP,
        options.headers
      )
    }

    // this is to ensure that queue is not overflown and work will not
    // be completed later on
    publishMessage
      .call(
        this,
        routing,
        message,
        options,
        parentSpan
      )
      .then(() => {
        this.log.trace({ latency: latency(time) }, 'message published')
      })
      .catch((err: Error) => {
        this.log.error({ err }, 'error sending message')
        this.#replyStorage.reject(correlationId, err)
      })

    return publishPromise
  }

  /**
   * @deprecated
   * @param {Function} messageHandler
   * @param {Array} listen
   * @param {Object} options
   */
  async createConsumedQueue<
    RequestBody extends any,
    ResponseBody extends any
    >(
    messageHandler: MessageHandler<RequestBody, ResponseBody>,
    listen: string[] = [],
    options: Partial<QueueConfOpts> & Pick<ConsumerOpts, 'neck'> & { queue?: string } = {}
  ) {
    const {
      queue,
      neck,
      ...queueOpts
    } = options

    return this.$createConsumedQueue(
      messageHandler,
      listen,
      {
        queue,
        queueOpts,
        consumerOpts: {
          neck,
        },
      }
    )
  }

  async $createConsumedQueue<
    RequestBody extends any,
    ResponseBody extends any
  >(
    messageHandler: MessageHandler<RequestBody, ResponseBody>,
    listen: string[] = [],
    options: {
      queue?: string
      queueOpts?: Partial<QueueConfOpts>
      consumerOpts?: Pick<ConsumerOpts, 'neck'>
    } = {}
  ) {
    if (is.function_(messageHandler) === false || is.array(listen) === false) {
      throw new ArgumentError('messageHandler and listen must be present')
    }

    if (is.object(options) === false) {
      throw new ArgumentError('options')
    }

    const { config } = this
    const router = initRoutingFn(messageHandler, this)

    const queueOpts: QueueOpts = {
      queue: options.queue ?? config.queue ?? '',
      options: merge(
        config.defaultQueueOpts,
        this.#extraQueueOpts,
      )
    }

    const consumerOpts: Omit<ConsumerOpts, 'queue'> = {
      neck: options.consumerOpts?.neck ?? config.neck,
      onMessage: router
    }

    const exchangesOpts: { regular: AnyExchangeOpts, headers?: AnyExchangeOpts } = {
      regular: {
        exchange: config.exchange,
        exchangeArgs: config.exchangeArgs,
      }
    }

    if (config.bindPersistentQueueToHeadersExchange) {
      for (const route of listen.values()) {
        assert(
          ExchangeFactory.isValidHeadersExchangeRoute(route),
          'with bindPersistentQueueToHeadersExchange: true routes must not have patterns'
        )
      }

      exchangesOpts.headers = config.headersExchange
    }

    // pipeline for establishing consumer
    const establishConsumer = async (attempt = 0): Promise<string> => {
      const { log } = this

      log.debug({ attempt }, 'establish consumer')
      const oldConsumer = this.#consumers!
        .get(establishConsumer)
      const oldQueue = this.#queues!
        .get(establishConsumer)

      // if we have old consumer
      if (oldConsumer) {
        await this.#consumers.close(oldConsumer)
      }

      let createdQueue
      try {
        const { queue } = createdQueue = await this.#createQueue(
          queueOpts,
          consumerOpts
        )

        await this.#exchangeFactory.bindQueueOnRoutes(
          queue,
          listen,
          exchangesOpts,
          oldQueue
        )
      } catch (e) {
        const err = new ConnectionError('failed to init queue or exchange', e)
        log.warn({ err }, '[consumed-queue-down]')
        await delay(this.#recovery.get(BackoffPolicy.Consumed, attempt + 1))
        return establishConsumer(attempt + 1)
      }

      const { consumer, queue } = createdQueue

      // save refs
      this.#reconnectionHandlers.set(consumer, establishConsumer)
      this.#queues.store(establishConsumer, queue)
      this.#consumers.store(establishConsumer, consumer)

      // emit event consumer & queue is ready
      const queueName = queue.name
      log.info({ queueName, consumerTag: consumer.consumerTag }, 'consumed-queue-reconnected')
      this.emit(AMQPTransportEvents.ConsumedQueueReconnected, consumer, queue, establishConsumer)

      return queueName
    }

    // make sure we recreate a queue and establish consumer on reconnect
    this.log.debug({ listen, queue: queueOpts.queue }, 'creating consumed queue')
    const queueName = await establishConsumer()

    this.log.debug({ listen, queue: queueName }, 'bound `ready` to establishConsumer')
    this.on(AMQPTransportEvents.Ready, establishConsumer)

    return establishConsumer
  }


  /**
   * Create queue with specified settings in current connection
   * also emit new event on message in queue
   *
   * @param {Object}  opts   - queue parameters
   */
  async createQueue(opts: string | (QueueOpts & { router: ReturnType<typeof initRoutingFn> })){
    const queueOpts = QueueFactory.ensureQueueOpts(opts)
    const consumerOpts: Omit<ConsumerOpts, 'queue'> = is.string(opts)
      ? {} as ConsumerOpts
      : {
        onMessage: opts.router,
        onMessagePre: this.#handleMessagePre,
        onError: this.#handleConsumerError,
        onClose: this.#handleConsumerClose,
        onCancel: this.#rebindConsumer,
      }

    return this.#createQueue(queueOpts, consumerOpts)
  }

  /**
   * Create unnamed private queue (used for reply events)
   */
  async createPrivateQueue(attempt = 0): Promise<ConsumedQueue> {
    try {
      return this.#createPrivateQueue()
    } catch (err) {
      const to = this.#recovery.get(BackoffPolicy.Private, attempt)

      this.log.error(
        { timeout: to, err, attempt },
        'private queue creation failed - restarting'
      )

      await delay(to)
      return this.createPrivateQueue(attempt + 1)
    }
  }

  /**
   * Noop function with empty correlation id and reply to data
   * @param  {Error} error
   * @param  {mixed} data
   * @param  {Span}  [span]
   * @param  {AMQPMessage} [raw]
   */
  noop(error: Error, data: any, span: opentracing.Span | undefined, raw: RawMessage<any>) {
    const msg = stringify({ error, data }, jsonSerializer)
    this.log.debug('when replying to message with %s response could not be delivered', msg)

    if (span !== undefined) {
      if (error) {
        span.setTag(Tags.ERROR, true)
        span.log({
          event: 'error',
          'error.object': error,
          message: error.message,
          stack: error.stack,
        })
      }

      span.finish()
    }

    if (raw !== undefined) {
      this.emit(AMQPTransportEvents.After, raw)
    }
  }

  /**
   * Reply to sender queue based on headers
   *
   * @param   {Object} properties - incoming message headers
   * @param   {mixed}  message - message to send
   * @param   {Span}   [span] - opentracing span
   * @param   {AMQPMessage} [raw] - raw message
   */
  reply(properties: PublishOptions, message: any, span?: opentracing.Span, raw?: RawMessage<any>) {
    if (!properties.replyTo || !properties.correlationId) {
      const error = new ValidationError('replyTo and correlationId not found in properties', '400')

      if (span !== undefined) {
        span.setTag(Tags.ERROR, true)
        span.log({
          event: 'error',
          'error.object': error,
          message: error.message,
          stack: error.stack,
        })
        span.finish()
      }

      if (raw !== undefined) {
        this.emit(AMQPTransportEvents.After, raw)
      }

      return Promise.reject(error)
    }

    const options = MessageOptions.getPublishOptions({
      correlationId: properties.correlationId,
    })

    if (properties[kReplyHeaders]) {
      options.headers = properties[kReplyHeaders]
    }

    let promise = this.send(properties.replyTo, message, options, span)

    if (raw !== undefined) {
      promise = promise
        .finally(() => this.emit(AMQPTransportEvents.After, raw))
    }

    return span === undefined
      ? promise
      : wrapPromise(span, promise)
  }

  #createQueue = async <
    $ConsumerOpts extends Partial<Omit<ConsumerOpts, 'queue'>> = Partial<Omit<ConsumerOpts, 'queue'>>
  >(
    queueOpts: QueueOpts,
    consumerOpts: $ConsumerOpts,
    consumerType = Consumer
  ): Promise<$ConsumerOpts extends Omit<ConsumerOpts, 'queue'>
      ? Promise<Required<ConsumedQueue>>
      : Promise<ConsumedQueue>
  > => {
    const queue = await this.#queues.create(queueOpts)
    const context: ConsumedQueue = {
      queue,
      options: queue.options,
    }

    if (typeof consumerOpts.onMessage !== 'function') {
      return context
    }

    const consumer = await this.#consumers.create(consumerType, {
      queue,
      onMessage: consumerOpts.onMessage,
      ...consumerOpts,
    })

    return {
      consumer,
      ...context,
    }
  }

  #createPrivateQueue = async () => {
    const queueName = Queue.getName(this.#replyTo as string | undefined)
    // reset current state
    this.#replyTo = undefined

    const queueOpts = {
      queue: queueName,
      options: this.config.privateQueueOpts,
    }

    const consumerOpts = {
      onError: this.#handlePrivateConsumerError,
      onCancel: this.#handlePrivateConsumerCancel,
      onMessage: this.#handlePrivateMessage,
    }

    const context = await this.#createQueue(
      queueOpts,
      consumerOpts,
      PrivateConsumer
    )

    // declare _replyTo queueName
    this.#replyTo = context.queue.name

    // bind temporary queue to headers exchange for DLX messages
    // NOTE: if this fails we might have a problem where expired messages
    // are not delivered & private queue is never ready
    if (this.config.dlx.enabled) {
      await this.#exchangeFactory.bindHeadersExchange(
        context.queue,
        this.#replyTo,
        this.config.dlx.params,
        WellKnowHeaders.ReplyTo
      )
    }

    // notify
    this.log.debug({ queue: this.#replyTo }, AMQPTransportEvents.PrivateQueueReady)
    this.emit(AMQPTransportEvents.PrivateQueueReady)

    return context
  }

  #awaitPrivateQueue = (): Promise<void> => {
    return new Promise((resolve, reject) => {
      let done: AnyFunction | null
      let error: AnyFunction | null

      done = function onReady(this: AMQPTransport) {
        this.removeAllListeners(AMQPTransportEvents.Error)
        error = null
        resolve()
      }

      error = function onError(this: AMQPTransport, err) {
        this.removeListener(AMQPTransportEvents.PrivateQueueReady, done as AnyFunction)
        done = null
        reject(err)
      }

      this.once(AMQPTransportEvents.PrivateQueueReady, done)
      this.once(AMQPTransportEvents.Error, error)
    })
  }

  // #onMessage: = <RequestBody extends any, ResponseBody extends any>()
  //
  /**
   * Stops consumers and closes transport
   */
  #close = async () => {
    await this.#closeAllConsumers()

    const amqp = this.#amqp
    if (amqp === null) {
      return
    }

    try {
      await new Promise((resolve, reject) => {
        amqp.once('close', resolve)
        amqp.once('error', reject)
        amqp.close()
      });
    } finally {
      amqp.removeAllListeners()
      this.#amqp = null
    }
  }

  #init = () => {
    if (this.#amqp === null) {
      throw new Error('Failed to call #init() on uninitialized instance')
    }

    const it = {
      amqp: this.#amqp,
    }

    this.#queues = new QueueFactory(it)
    this.#consumers = new ConsumerFactory(it)
    this.#exchangeFactory = new ExchangeFactory(it)
  }

  #closeAllConsumers = async () => {
    const work = [];

    for (const consumer of this.#consumers.values()) {
      work.push(this.#stopConsumedQueue(consumer))
    }

    await Promise.all(work)
  }

  /**
   * Prevents consumer from re-establishing connection
   * @param {Consumer} consumer
   * @returns {Promise<Void>}
   */
  #stopConsumedQueue = async (consumer: Consumer) => {
    if (!consumer) {
      throw new TypeError('consumer must be defined')
    }

    const establishConsumer = this.#reconnectionHandlers.get(consumer)
    this.log.debug({ establishConsumer: !!establishConsumer }, 'fetched establish consumer')

    if (establishConsumer) {
      this.removeListener(AMQPTransportEvents.Ready, establishConsumer)
    }

    await consumer.close()
  }

  /**
   * 'ready' event from amqp-coffee lib, perform queue recreation here
   */
  #onConnect = () => {
    const { serverProperties } = (this.#amqp as AMQP.Instance)
    const { cluster_name: clusterName, version } = serverProperties

    // emit connect event through log
    this.log.info('connected to %s v%s', clusterName, version)

    // init handlers for queues and consumers
    this.#init()

    // https://github.com/dropbox/amqp-coffee#reconnect-flow
    // recreate unnamed private queue
    // replyTo === false when reconnecting
    if ((this.#replyTo || this.config.private) && this.#replyTo !== false) {
      this.createPrivateQueue()
        .then(() => {
          this.log.info({ queue: this.#replyTo }, 'a private queue is created')
        })
    }

    // re-emit ready
    this.emit(AMQPTransportEvents.Ready)
  }

  /**
   * Pass in close event
   */
  #onClose = (err: Error) => {
    // emit connect event through log
    this.log.warn({ err }, 'connection is closed')
    // re-emit close event
    this.emit(AMQPTransportEvents.Close, err)
  }

  #handleMessagePre = <RequestBody>(raw: RawMessage<RequestBody>)  => {
    this.emit(AMQPTransportEvents.Pre, raw)
  }

  #handlePrivateConsumerError = (_: Consumer, shouldRecreate: boolean, err: AMQP.ConsumerError): void => {
    if (shouldRecreate && !this.isConnecting) {
      this.createPrivateQueue()
      return
    }

    this.log.error('private consumer returned error', err)
    this.emit(AMQPTransportEvents.Error, err)
  }

  #handlePrivateConsumerCancel = () => {
    if (!this.isConnecting) {
      this.createPrivateQueue()
    }
  }

  /**
   * Distributes messages from a private queue
   * @param  {mixed}  message
   * @param  {Object} properties
   */
  #handlePrivateMessage = async <
    Body extends any,
  >(
    message: Message<Body>,
    properties: PublishOptions,
  ): Promise<Body | null | void> => {
    const { correlationId, replyTo, headers } = properties
    const { [WellKnowHeaders.XDeath]: xDeath } = headers

    // retrieve promised message
    const future = this.#replyStorage.pop(correlationId)

    // case 1 - for some reason there is no saved reference, example - crashed process
    if (future === undefined) {
      this.log.error(
        'no recipient for the message %j and id %s',
        message.error || message.data || message,
        correlationId
      )

      let error
      if (xDeath) {
        error = new AmqpDLXError(xDeath, message)
        this.log.warn('message was not processed', error)
      }

      // otherwise we just run messages in circles
      if (replyTo && replyTo !== this.#replyTo) {
        // if error is undefined - generate this
        if (error === undefined) {
          error = new NotPermittedError(`no recipients found for correlationId "${correlationId}"`)
        }

        // reply with the error
        return this.reply(properties, { error })
      }

      // we are done
      return null
    }

    this.log.trace('response returned in %s', latency(future.time))

    // if message was dead-lettered - reject with an error
    if (xDeath) {
      return future.reject(new AmqpDLXError(xDeath, message))
    }

    if (message.error) {
      const error = wrapError(message.error)

      Object.defineProperty(error, kReplyHeaders, {
        value: headers,
        enumerable: false,
      })

      return future.reject(error)
    }

    const response = buildResponse(message, properties)
    const adaptedResponse = adaptResponse(response, future.replyOptions)

    if (future.cache) {
      this.#cache.set(future.cache, response)
    }

    return future.resolve(adaptedResponse)
  }

  #handleConsumerError = (
    consumer: Consumer,
    shouldRecreate: boolean,
    err: AMQP.ConsumerError,
    res: any
  ): void => {
    if (shouldRecreate) {
      this.#rebindConsumer(consumer, err, res)
    }
  }

  #handleConsumerClose = (consumer: Consumer) => {
    this.emit(AMQPTransportEvents.ConsumerClose, consumer.consumer)
  }

  #rebindConsumer = async (consumer: Consumer, err: AMQP.ConsumerError, res: any) => {
    const msg = err ? err.replyText : 'uncertain'

    // cleanup a bit
    this.log.warn({ err, res }, 're-establishing connection after %s', msg)
    const reconnect = this.#reconnectionHandlers
      .get(consumer)

    try {
      await this.#consumers.close(consumer)
      await delay(this.#recovery.get(BackoffPolicy.Consumed, 1))
    } catch (e) {
      this.log.error({ err: e }, 'failed to close consumer')
    } finally {
      if (is.undefined(reconnect)) {
        this.log.fatal({ err, res }, 'failed to fetch connection handler')
        return
      }

      await reconnect()
    }
  }
}
