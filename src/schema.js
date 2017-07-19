const baseJoi = require('joi');
const is = require('is');
const recoverySchema = require('./utils/recovery').schema;

const Joi = baseJoi.extend({
  name: 'coercedArray',
  base: baseJoi.array().items(baseJoi.string()).unique(),
  // eslint-disable-next-line no-unused-vars
  coerce(value, state, options) {
    if (is.string(value)) {
      return [value];
    }

    return value;
  },
});

const exchangeTypes = Joi.string()
  .only('direct', 'topic', 'headers', 'fanout');

module.exports = Joi.object({
  name: Joi.string()
    .default('amqp', 'name of the service when advertising to AMQP'),

  private: Joi.boolean()
    .default(false, 'when true - initializes private queue right away'),

  cache: Joi.number().min(0)
    .default(100, 'size of LRU cache for responses, 0 to disable it'),

  timeout: Joi.number()
    .default(10000, 'default *AndWait timeout'),

  debug: Joi.boolean()
    .default(process.env.NODE_ENV === 'development', 'enables debug messages'),

  listen: Joi.coercedArray()
    .description('attach default queue to these routes on default exchange'),

  version: Joi.string()
    .default('n/a', 'advertise end-client service version'),

  neck: Joi.number().min(0)
    .description('if defined - queues will enter QoS mode with required ack & prefetch size of neck'),

  tracer: Joi.object(),

  connection: Joi
    .object({
      host: Joi.alternatives()
        .try(
          Joi.string(),
          Joi.array().min(1).items(Joi.string()),
          Joi.array().min(1).items(Joi.object({
            host: Joi.string().required(),
            port: Joi.number().required(),
          }))
        )
        .default('localhost', 'rabbitmq host'),

      port: Joi.number()
        .default(5672, 'rabbitmq port'),

      heartbeat: Joi.number()
        .default(10000, 'heartbeat check'),

      login: Joi.string()
        .default('guest', 'rabbitmq login'),

      password: Joi.string()
        .default('guest', 'rabbitmq password'),

      vhost: Joi.string()
        .default('/', 'rabbitmq virtual host'),

      temporaryChannelTimeout: Joi.number()
        .default(6000, 'temporary channel close time with no activity'),

      reconnect: Joi.boolean()
        .default(true, 'enable auto-reconnect'),

      reconnectDelayTime: Joi.number()
        .default(500, 'reconnect delay time'),

      hostRandom: Joi.boolean()
        .default(false, 'select host to connect to randomly'),

      ssl: Joi.boolean()
        .default(false, 'whether to use SSL'),

      sslOptions: Joi.object()
        .description('ssl options'),

      noDelay: Joi.boolean()
        .default(true, 'disable Nagle\'s algorithm'),

      clientProperties: Joi
        .object({
          capabilities: Joi.object({
            consumer_cancel_notify: Joi.boolean()
              .default(true, 'whether to react to cancel events'),
          }).default(),
        })
        .description('options for advertising client properties')
        .default(),
    })
    .description('options for setting up connection to RabbitMQ')
    .default(),

  recovery: recoverySchema
    .description('recovery settings')
    .default(),

  exchange: Joi.string()
    .allow('')
    .default('node-services', 'default exchange for communication'),

  exchangeArgs: Joi
    .object({
      autoDelete: Joi.boolean()
        .default(false, 'do not autoDelete exchanges'),

      noWait: Joi.boolean()
        .default(false, 'whether not to wait for declare response'),

      internal: Joi.boolean()
        .default(false, 'whether to set internal bit'),

      type: exchangeTypes
        .default('topic', 'type of the exchange'),

      durable: Joi.boolean()
        .default(true, 'whether to preserve exchange on rabbitmq restart'),
    })
    .default(),

  bindPersistantQueueToHeadersExchange: Joi.boolean()
    .default(false, 'whether to bind queues created by .createConsumedQueue to headersExchange'),

  headersExchange: Joi
    .object({
      exchange: Joi.string()
        .default('amq.headers', 'default headers exchange to use'),

      autoDelete: Joi.boolean()
        .default(false, 'do not autoDelete exchanges'),

      noWait: Joi.boolean()
        .default(false, 'whether not to wait for declare response'),

      internal: Joi.boolean()
        .default(false, 'whether to set internal bit'),

      type: Joi.string()
        .only('headers')
        .default('headers', 'type of the exchange'),

      durable: Joi.boolean()
        .default(true, 'whether to preserve exchange on rabbitmq restart'),
    })
    .description('this exchange is used to support delayed retry with QoS exchanges')
    .default(),

  queue: Joi.string()
    .description('default queue to connect to for consumption'),

  defaultQueueOpts: Joi
    .object({
      autoDelete: Joi.boolean(),

      exclusive: Joi.boolean(),

      noWait: Joi.boolean(),

      passive: Joi.boolean(),

      durable: Joi.boolean()
        .default(true, 'survive restarts & use disk storage'),

      arguments: Joi
        .object({
          'x-expires': Joi.number().min(0)
            .description('delete queue after it\'s been unused for X seconds'),

          'x-max-priority': Joi.number().min(2).max(255)
            .description('setup priority queues where messages will be delivery based on priority level'),
        })
        .default(),
    })
    .description('default options for creating consumer queues')
    .default(),

  privateQueueOpts: Joi
    .object({
      autoDelete: Joi.boolean(),

      exclusive: Joi.boolean(),

      noWait: Joi.boolean(),

      passive: Joi.boolean(),

      durable: Joi.boolean()
        .default(true, 'survive restarts & use disk storage'),

      arguments: Joi
        .object({
          'x-expires': Joi.number().min(0)
            .default(1800000, 'delete the private queue after it\'s been unused for 3 minutes'),

          'x-max-priority': Joi.number().min(2).max(255)
            .description('setup priority queues where messages will be delivery based on priority level'),
        })
        .default(),
    })
    .description('default options for private RPC queues')
    .default(),

  dlx: Joi
    .object({
      enabled: Joi.boolean()
        .default(true, 'enabled DLX by default for fast-reply when messages are dropped'),

      params: Joi
        .object({
          exchange: Joi.string()
            .default('amq.headers', 'dead letters are redirected here'),

          type: exchangeTypes
            .default('headers', 'must be headers for proper built-in matching'),

          autoDelete: Joi.boolean()
            .default(false, 'DLX persistance'),
        })
        .default(),
    })
    .description('default for dead-letter-exchange')
    .default(),

  defaultOpts: Joi
    .object({
      deliveryMode: Joi.number().only(1, 2)
        .default(1, '1 - transient, 2 - saved on disk'),

      confirm: Joi.boolean()
        .default(false, 'whether to wait for commit confirmation'),

      mandatory: Joi.boolean()
        .default(false, 'when true and message cant be routed to a queue - exception returned, otherwise its dropped'),

      immediate: Joi.boolean()
        .default(false, 'not implemented by rabbitmq'),

      headers: Joi.object()
        .default(),
    })
    .description('default options when publishing messages')
    .default(),
});
