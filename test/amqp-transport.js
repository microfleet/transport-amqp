/* eslint-disable no-console, max-len, promise/always-return */

const Promise = require('bluebird');
const Proxy = require('@microfleet/amqp-coffee/test/proxy').route;
const ld = require('lodash');
const stringify = require('json-stringify-safe');
const sinon = require('sinon');
const assert = require('assert');
const microtime = require('microtime');

describe('AMQPTransport', function AMQPTransportTestSuite() {
  // require module
  const AMQPTransport = require('../src');
  const { AmqpDLXError } = require('../src/utils/error');
  const { jsonSerializer, jsonDeserializer } = require('../src/utils/serialization');
  const latency = require('../src/utils/latency');

  const configuration = {
    exchange: 'test-exchange',
    debug: true,
    connection: {
      host: process.env.RABBITMQ_PORT_5672_TCP_ADDR || 'localhost',
      port: process.env.RABBITMQ_PORT_5672_TCP_PORT || 5672,
    },
  };

  it('stringifies message correctly', () => {
    this.originalMsg = {
      meta: {
        controlsData: [
          0.25531813502311707, 0.0011256206780672073, 0.06426551938056946,
          -0.001104108989238739, 0.852259635925293, 0.005791602656245232,
          -0.5230863690376282, 0, 0.9999388456344604, 0.011071242392063141,
          0.523118257522583, -0.009435615502297878, 0.8522077798843384,
          0.8522599935531616, 0, 0.5231184363365173, 0, 0.005791574250906706,
          0.9999387264251709, -0.009435582906007767, 0, -0.5230863690376282,
          0.011071248911321163, 0.8522077798843384, 0, -0.13242781162261963,
          0.06709221005439758, 0.21647998690605164, 1,
        ],
        name: 'oki-dokie',
      },
      body: {
        random: true,
        data: [{
          filename: 'ok',
          version: 10.3,
        }],
      },
      buffer: Buffer.from('xxx'),
    };

    this.msg = stringify(this.originalMsg, jsonSerializer);

    // eslint-disable-next-line max-len
    assert.equal(this.msg, '{"meta":{"controlsData":[0.25531813502311707,0.0011256206780672073,0.06426551938056946,-0.001104108989238739,0.852259635925293,0.005791602656245232,-0.5230863690376282,0,0.9999388456344604,0.011071242392063141,0.523118257522583,-0.009435615502297878,0.8522077798843384,0.8522599935531616,0,0.5231184363365173,0,0.005791574250906706,0.9999387264251709,-0.009435582906007767,0,-0.5230863690376282,0.011071248911321163,0.8522077798843384,0,-0.13242781162261963,0.06709221005439758,0.21647998690605164,1],"name":"oki-dokie"},"body":{"random":true,"data":[{"filename":"ok","version":10.3}]},"buffer":{"type":"Buffer","data":[120,120,120]}}');
  });

  it('deserializes message correctly', () => {
    assert.deepEqual(JSON.parse(this.msg, jsonDeserializer), this.originalMsg);
  });

  it('serializes & deserializes error', () => {
    const serialized = stringify(new Error('ok'), jsonSerializer);
    const err = JSON.parse(serialized, jsonDeserializer);

    assert.equal(err.name, 'MSError');
    assert.equal(!!err.stack, true);
    assert.equal(err.message, 'ok');
  });

  it('is able to be initialized', () => {
    const amqp = new AMQPTransport(configuration);
    assert(amqp instanceof AMQPTransport);
    assert(amqp.config, 'config defined');
    assert(amqp.replyStorage, 'reply storage initialized');
    assert(amqp.cache, 'cache storage initialized');
  });

  it('fails on invalid configuration', () => {
    function createTransport() {
      return new AMQPTransport({
        name: {},
        private: 'the-event',
        exchange: '',
        timeout: 'don-don',
        connection: 'bad option',
      });
    }

    assert.throws(createTransport, 'ValidationError');
  });

  it('is able to connect to rabbitmq', () => {
    const amqp = this.amqp = new AMQPTransport(configuration);
    return amqp.connect()
      .then(() => {
        assert.equal(amqp._amqp.state, 'open');
      });
  });

  it('is able to disconnect', () => (
    this.amqp.close().then(() => {
      assert.equal(this.amqp._amqp, null);
    })
  ));

  it('is able to connect via helper function', () => (
    AMQPTransport
      .connect(configuration)
      .then((amqp) => {
        assert.equal(amqp._amqp.state, 'open');
        this.amqp = amqp;
      })
  ));

  it('is able to consume routes', () => {
    const opts = {
      debug: true,
      cache: 100,
      exchange: configuration.exchange,
      queue: 'test-queue',
      listen: 'test.default',
      connection: configuration.connection,
    };

    return AMQPTransport
      .connect(opts, function listener(message, headers, actions, callback) {
        callback(null, { resp: `${message}-response`, time: process.hrtime() });
      })
      .then((amqp) => {
        assert.equal(amqp._amqp.state, 'open');
        this.amqp_consumer = amqp;
      });
  });

  it('is able to publish to route consumer', () => (
    this.amqp
      .publishAndWait('test.default', 'test-message')
      .then((response) => {
        assert.equal(response.resp, 'test-message-response');
      })
  ));

  it('is able to publish to route consumer:2', () => (
    this.amqp
      .publishAndWait('test.default', 'test-message')
      .then((response) => {
        assert.equal(response.resp, 'test-message-response');
      })
  ));

  it('is able to publish to route consumer:2', () => (
    this.amqp
      .publishAndWait('test.default', 'test-message')
      .then((response) => {
        assert.equal(response.resp, 'test-message-response');
      })
  ));

  it('is able to send messages directly to a queue', () => {
    const privateQueue = this.amqp._replyTo;
    return this.amqp_consumer
      .sendAndWait(privateQueue, 'test-message-direct-queue')
      .reflect()
      .then((promise) => {
        assert.equal(promise.isRejected(), true);
        assert.equal(promise.reason().name, 'NotPermittedError');
      });
  });

  describe('concurrent publish', () => {
    before('init consumer', () => {
      const transport = this.concurrent = new AMQPTransport(configuration);
      return transport.connect();
    });

    it('able to publish multiple messages at once', () => {
      const transport = this.concurrent;
      const promises = ld.times(5, i => (
        transport.publishAndWait('test.default', `ok.${i}`)
      ));
      return Promise.all(promises);
    });

    after('close consumer', () => (
      this.concurrent.close()
    ));
  });

  describe('DLX: enabled', () => {
    before('init amqp', () => {
      const transport = this.dlx = new AMQPTransport(configuration);
      return transport.connect();
    });

    it('create queue, but do not consume', () => (
      this.dlx.createConsumedQueue(() => {}, ['hub'], {
        queue: 'dlx-consumer',
      })
      .spread(consumer => consumer.close())
    ));

    it('publish message and receive DLX response', () => (
      // it will be published to the `dlx-consumer` queue
      // and after 2250 ms moved to '' with routing key based on the
      // headers values
      this.dlx.publishAndWait('hub', { wont: 'be-consumed-queue' }, {
        // set smaller timeout than 10s so we don't wait
        // resulting x-message-ttl is 80% (?) of timeout
        timeout: 2500,
      })
      .throw(new Error('did not reject'))
      .catch(AmqpDLXError, (e) => {
        assert.equal(e.message, 'Expired from queue "dlx-consumer" with routing keys ["hub"] after 2250ms 1 time(s)');
      })
    ));

    after('close amqp', () => (
      this.dlx.close()
    ));
  });

  describe('cached request', () => {
    before('init consumer', () => {
      const transport = this.cached = new AMQPTransport(configuration);
      return transport.connect();
    });

    it('publishes batches of messages, they must return cached values and then new ones', () => {
      const transport = this.cached;
      const publish = () => transport.publishAndWait('test.default', 1, { cache: 2000 });
      const promises = [
        publish(),
        Promise.delay(300).then(publish),
        Promise.delay(5000).then(publish),
      ];

      return Promise.all(promises).spread((initial, cached, nonCached) => {
        const toMiliseconds = latency.toMiliseconds;
        assert.equal(toMiliseconds(initial.time), toMiliseconds(cached.time));
        assert(toMiliseconds(initial.time) < toMiliseconds(nonCached.time));
      });
    });

    after('close published', () => (
      this.cached.close()
    ));
  });

  describe('AMQPTransport.multiConnect', () => {
    let acksCalled = 0;

    const conf = {
      debug: true,
      exchange: configuration.exchange,
      connection: configuration.connection,
      queue: 'multi',
      listen: ['t.#', 'tbone', 'morgue'],
    };

    it('initializes amqp instance', () => {
      // mirrors all messages
      const spy = this.spy = sinon.spy(function listener(message, headers, actions, callback) {
        if (actions && actions.ack) {
          acksCalled += 1;
          actions.ack();
        }

        callback(null, message);
      });

      const consumer = AMQPTransport.multiConnect(conf, spy, [{
        neck: 1,
      }]);

      const publisher = AMQPTransport.connect(configuration);

      return Promise.join(consumer, publisher, (multi, amqp) => {
        this.multi = multi;
        this.publisher = amqp;
      });
    });

    it('verify that messages are all received & acked', () => {
      const q1 = Array.from({ length: 100 }).map((_, idx) => ({
        route: `t.${idx}`,
        message: `t.${idx}`,
      }));

      const q2 = Array.from({ length: 20 }).map((_, idx) => ({
        route: 'tbone',
        message: `tbone.${idx}`,
      }));

      const q3 = Array.from({ length: 30 }).map((_, idx) => ({
        route: 'morgue',
        message: `morgue.${idx}`,
      }));

      const pub = [...q1, ...q2, ...q3];

      return Promise.map(pub, message => (
        this.publisher.publishAndWait(message.route, message.message)
      ))
      .then((responses) => {
        assert.equal(acksCalled, q1.length);

        // ensure all responses match
        pub.forEach((p, idx) => {
          assert.equal(responses[idx], p.message);
        });

        assert.equal(this.spy.callCount, pub.length);
      });
    });

    after('close multi-transport', () => (
      Promise.join(
        this.multi.close(),
        this.publisher.close()
      )
    ));
  });

  describe('priority queue', function test() {
    const conf = {
      debug: true,
      exchange: configuration.exchange,
      connection: configuration.connection,
      queue: 'priority',
    };

    it('initializes amqp instance', () => {
      // mirrors all messages
      const consumer = AMQPTransport.connect(conf);
      const publisher = AMQPTransport.connect(configuration);

      return Promise.join(consumer, publisher, (priority, amqp) => {
        this.priority = priority;
        this.publisher = amqp;
      });
    });

    it('create priority queue', () => {
      return this.priority.createQueue({
        queue: 'priority',
        arguments: {
          'x-max-priority': 5,
        },
      })
      .then(({ queue }) => (
        this.priority.bindExchange(queue, ['priority'], this.priority.config.exchangeArgs)
      ));
    });

    it('prioritize messages', () => {
      const messages = Array.from({ length: 3 }).map((_, idx) => ({
        message: idx % 4,
        priority: idx % 4,
      }));

      const spy = sinon.spy(function listener(message, headers, actions, callback) {
        actions.ack();
        callback(null, microtime.now());
      });

      const publish = Promise.map(messages, ({ message, priority }) => {
        return this.publisher.publishAndWait('priority', message, { priority, confirm: true, timeout: 60000 });
      });

      const consume = Promise.delay(500).then(() => this.priority.createConsumedQueue(spy, ['priority'], {
        neck: 1,
        arguments: {
          'x-max-priority': 5,
        },
      }));

      return Promise.join(publish, consume, (data) => {
        data.forEach((micro, idx) => {
          if (data[idx + 1]) assert.ok(micro > data[idx + 1]);
        });
      });
    });

    after('close priority-transport', () => (
      Promise.join(
        this.priority.close(),
        this.publisher.close()
      )
    ));
  });

  describe('consumed queue', function test() {
    before('init transport', () => {
      this.proxy = new Proxy(9010, 5672, 'localhost');
      this.transport = new AMQPTransport({
        debug: true,
        connection: {
          port: 9010,
          heartbeat: 2000,
        },
        exchange: 'test-direct',
        exchangeArgs: {
          autoDelete: false,
          type: 'direct',
        },
        defaultQueueOpts: {
          autoDelete: true,
          exclusive: true,
        },
      });

      return this.transport.connect();
    });

    function router(message, headers, actions, next) {
      switch (headers.routingKey) {
        case '/':
          // #3 all right, try answer
          assert.deepEqual(message, { foo: 'bar' });
          return next(null, { bar: 'baz' });
        default:
          throw new Error();
      }
    }

    it('reestablishing consumed queue', () => {
      const transport = this.transport;
      const publish = () => transport.publishAndWait('/', { foo: 'bar' }, { confirm: true });

      return transport
        .createConsumedQueue(router, ['/'])
        .tap(() => Promise.all([
          publish(),
          Promise.delay(250).then(publish),
          Promise.delay(5000).then(publish),
          Promise.delay(300).then(() => this.proxy.interrupt(3000)),
        ]))
        .spread((consumer, queue, establishConsumer) => Promise.join(
          transport.stopConsumedQueue(consumer, establishConsumer),
          Promise.fromCallback(next => queue.delete(next))
        ));
    });

    it('should create consumed queue', (done) => {
      const transport = this.transport;
      transport.on('consumed-queue-reconnected', (consumer, queue) => (
        // #2 reconnected, try publish
        transport
          .publishAndWait('/', { foo: 'bar' })
          .then((message) => {
            // #4 OK, try unbind
            assert.deepEqual(message, { bar: 'baz' });
            return transport.unbindExchange(queue, '/');
          })
          .then(() => {
            // #5 unbinded, let's reconnect
            transport.removeAllListeners('consumed-queue-reconnected');
            transport.on('consumed-queue-reconnected', () => {
              // #7 reconnected again
              transport.publish('/', { bar: 'foo' });
              Promise.delay(1000).tap(done);
            });

            // #6 trigger error again
            setTimeout(() => {
              this.proxy.interrupt(20);
            }, 10);
          })
          .catch(() => {})
      ));

      transport.createConsumedQueue(router)
        .spread((consumer, queue) => transport.bindExchange(queue, '/'))
        .tap(() => {
          // #1 trigger error
          setTimeout(() => {
            this.proxy.interrupt(20);
          }, 10);
        });
    });

    after('close transport', () => {
      this.proxy.close();
      this.transport.close();
    });
  });

  after('cleanup', () => (
    Promise.map(['amqp', 'amqp_consumer'], name => (
      this[name] && this[name].close()
    ))
  ));
});
