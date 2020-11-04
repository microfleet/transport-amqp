import { deepStrictEqual, throws } from 'assert'

import * as sh from '../src/schema/helpers'
import { Schema } from '../src/schema'
import { QueueConfOpts } from '../src/queue'
import { ConnectionOpts, Host } from '../src/schema/connection'
import { DeadLetterExchangeOpts } from '../src/schema/dlx'
import { ExchangeConfOpts, ExchangeType } from '../src/exchange'
import { DeliveryMode, PublishingConfOpts } from '../src/schema/publishing'

describe('[schema helpers]', () => {
  it('should parse string as string[]', () => {
    deepStrictEqual(sh.CoercedStringArray.parse('asd'), ['asd'])
  })

  it('should parse string[] as string[]', () => {
    const array = [
      'asd',
      'fda',
    ]
    deepStrictEqual(sh.CoercedStringArray.parse(array), array)
  })

  it('should parse only unique items in the string[]', () => {
    const array = [
      'asd',
      'fda',
      'asd',
    ]

    throws(() => {
      sh.CoercedUniqStringArray.parse(array)
    })
  })
})

describe('[AMQP exchange opts]', () => {
  it('should validate && fill defaults', () => {
    const opts: Partial<ExchangeConfOpts> = {}
    const result = ExchangeConfOpts.parse(opts)
    const expected: ExchangeConfOpts = {
      noWait: false,
      durable: true,
      internal: false,
      autoDelete: false,
      type: ExchangeType.Topic,
    }

    deepStrictEqual(result, expected)
  })

  it('should also validate when some options passed', () => {
    const opts: Partial<ExchangeConfOpts> = {
      type: ExchangeType.Direct,
      noWait: true,
      durable: false,
      internal: true,
      autoDelete: true,
    }
    const result = ExchangeConfOpts.parse(opts)

    deepStrictEqual(result, opts)
  })
})

describe('[AMQP queue opts]', () => {
  it('should validate && fill defaults', () => {
    const opts: Partial<QueueConfOpts> = {}
    const result = QueueConfOpts.parse(opts)
    const expected: QueueConfOpts = {
      durable: true,
      arguments: {
        'x-expires': 1800000,
      }
    }

    deepStrictEqual(result, expected)
  })

  it('should also validate when some options passed', () => {
    const opts: Partial<QueueConfOpts> = {
      noWait: true,
      passive: false,
      durable: false,
      exclusive: false,
      autoDelete: true,
      arguments: {
        'x-expires': 500,
        'x-max-priority': 50,
      }
    }
    const result = QueueConfOpts.parse(opts)

    deepStrictEqual(result, opts)
  })
})

describe('[AMQP DLX opts]', () => {
  it('should validate && fill defaults', () => {
    const opts: Partial<DeadLetterExchangeOpts> = {}
    const result = DeadLetterExchangeOpts.parse(opts)

    deepStrictEqual(result, {
      enabled: true,
      params: {
        exchange: 'amq.headers',
        type: ExchangeType.Headers,
        autoDelete: false,
      }
    })
  })

  it('should also validate when some options passed', () => {
    const opts: Partial<DeadLetterExchangeOpts> = {
      enabled: false,
      params: {
        exchange: 'amq.headers.ex',
        type: ExchangeType.Fanout,
        autoDelete: true,
      }
    }
    const result = DeadLetterExchangeOpts.parse(opts)

    deepStrictEqual(result, opts)
  })
})

describe('[AMQP Connection opts && Host]', () => {
  it('[host] should return default', () => {
    // @ts-expect-error
    deepStrictEqual(Host.parse(), 'localhost')
  })

  it('[host] should accept string', () => {
    deepStrictEqual(Host.parse('rabbitmq.host'), 'rabbitmq.host')
  })

  it('[host] should accept array of strings', () => {
    deepStrictEqual(Host.parse(['rabbit', 'fox', 'moose']), [
      'rabbit',
      'fox',
      'moose'
    ])
  })

  it('[host] should accept array of objects', () => {
    deepStrictEqual(Host.parse([{
      host: 'rabbitmq',
      port: 5672,
    }]), [{
      host: 'rabbitmq',
      port: 5672,
    }])
  })

  it('[connection] should produce defaults', () => {
    deepStrictEqual(ConnectionOpts.parse({}), {
      host: 'localhost',
      port: 5672,
      heartbeat: 10000,
      login: 'guest',
      password: 'guest',
      vhost: '/',
      temporaryChannelTimeout: 6000,
      reconnect: true,
      reconnectDelayTime: 500,
      hostRandom: false,
      ssl: false,
      noDelay: true,
      clientProperties: {
        capabilities: {
          consumer_cancel_notify: true,
        },
      },
    })
  })
})

describe('[AMQP Publishing opts]', () => {
  it('should validate && fill defaults', () => {
    deepStrictEqual(PublishingConfOpts.parse({}), {
      deliveryMode: DeliveryMode.Transient,
      confirm: false,
      mandatory: false,
      immediate: false,
      contentType: 'application/json',
      contentEncoding: 'plain',
      simpleResponse: true,
    })
  })

  it('should also validate when some options passed', () => {
    deepStrictEqual(PublishingConfOpts.parse({
      deliveryMode: DeliveryMode.SaveOnDisc,
      confirm: true,
      mandatory: true,
      immediate: true,
      contentType: 'application/any',
      contentEncoding: 'gzip',
      simpleResponse: false,
    }), {
      deliveryMode: DeliveryMode.SaveOnDisc,
      confirm: true,
      mandatory: true,
      immediate: true,
      contentType: 'application/any',
      contentEncoding: 'gzip',
      simpleResponse: false,
    })
  })
})

describe('[AMQP config schema]', () => {
  it('should validate && fill defaults', () => {
    deepStrictEqual(Schema.parse({
      queue: 'q',
      listen: [
        'route.v1',
        'route.v2',
      ],
      neck: 0,
    }), {
      name: 'amqp',
      private: false,
      cache: 100,
      timeout: 10000,
      debug: true,
      listen: [
        'route.v1',
        'route.v2',
      ],
      version: 'n/a',
      neck: 0,
      connection: {
        host: 'localhost',
        port: 5672,
        heartbeat: 10000,
        login: 'guest',
        password: 'guest',
        vhost: '/',
        temporaryChannelTimeout: 6000,
        reconnect: true,
        reconnectDelayTime: 500,
        hostRandom: false,
        ssl: false,
        noDelay: true,
        clientProperties: {
          capabilities: {
            consumer_cancel_notify: true,
          },
        },
      },
      exchange: 'node-services',
      exchangeArgs: {
        noWait: false,
        durable: true,
        internal: false,
        autoDelete: false,
        type: ExchangeType.Topic,
      },
      bindPersistentQueueToHeadersExchange: false,
      headersExchange: {
        noWait: false,
        durable: true,
        internal: false,
        autoDelete: false,
        type: ExchangeType.Headers,
        exchange: 'amq.match',
      },
      queue: 'q',
      defaultQueueOpts: {
        durable: true,
        arguments: {
          'x-expires': 1800000,
        },
      },
      privateQueueOpts: {
        durable: true,
        arguments: {
          'x-expires': 1800000,
        },
      },
      dlx: {
        enabled: true,
        params: {
          exchange: 'amq.headers',
          type: ExchangeType.Headers,
          autoDelete: false,
        }
      },
      defaultOpts: {
        deliveryMode: DeliveryMode.Transient,
        confirm: false,
        mandatory: false,
        immediate: false,
        contentType: 'application/json',
        contentEncoding: 'plain',
        simpleResponse: true,
      }
    })
  })
})
