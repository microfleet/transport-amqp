/* eslint-disable no-console, max-len, promise/always-return */

const Promise = require('bluebird');
const Proxy = require('@microfleet/amqp-coffee/test/proxy').route;
const ld = require('lodash');
const stringify = require('json-stringify-safe');
const sinon = require('sinon');
const assert = require('assert');
const microtime = require('microtime');
const { MockTracer } = require('opentracing/lib/mock_tracer');
const debug = require('debug')('amqp');

// add inject/extract implementation
MockTracer.prototype._inject = (span, format, carrier) => {
  carrier['x-mock-span-uuid'] = span._span.uuid();
};

MockTracer.prototype._extract = function extract(format, carrier) {
  return this.report().spansByUUID[carrier['x-mock-span-uuid']];
};

/* eslint-disable no-restricted-syntax */
const printReport = (report) => {
  const reportData = ['Spans:'];
  for (const span of report.spans) {
    const tags = span.tags();
    const tagKeys = Object.keys(tags);

    reportData.push(`    ${span.operationName()} - ${span.durationMs()}ms`);
    for (const key of tagKeys) {
      const value = tags[key];
      reportData.push(`        tag '${key}':'${value}'`);
    }
  }

  if (report.unfinishedSpans.length > 0) reportData.push('Unfinished:');
  for (const unfinishedSpan of report.unfinishedSpans) {
    const tags = unfinishedSpan.tags();
    const tagKeys = Object.keys(tags);

    reportData.push(`    ${unfinishedSpan.operationName()} - ${unfinishedSpan.durationMs()}ms`);
    for (const key of tagKeys) {
      const value = tags[key];
      reportData.push(`        tag '${key}':'${value}'`);
    }
  }

  return reportData.join('\n');
};
/* eslint-enable no-restricted-syntax */

describe('AMQPTransport', function AMQPTransportTestSuite() {
  // require module
  const AMQPTransport = require('../src');
  const { AmqpDLXError } = require('../src/utils/error');
  const { jsonSerializer, jsonDeserializer } = require('../src/utils/serialization');
  const latency = require('../src/utils/latency');
  const { kReplyHeaders } = require('../src/constants');

  const RABBITMQ_HOST = process.env.RABBITMQ_PORT_5672_TCP_ADDR || 'localhost';
  const RABBITMQ_PORT = +(process.env.RABBITMQ_PORT_5672_TCP_PORT || 5672);

  const configuration = {
    exchange: 'test-exchange',
    connection: {
      host: RABBITMQ_HOST,
      port: RABBITMQ_PORT,
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
      cache: 100,
      exchange: configuration.exchange,
      queue: 'test-queue',
      listen: 'test.default',
      connection: configuration.connection,
    };

    return AMQPTransport
      .connect(opts, function listener(message, headers, actions, callback) {
        callback(null, {
          resp: typeof message === 'object' ? message : `${message}-response`,
          time: process.hrtime(),
        });
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
      this.dlx
        .createConsumedQueue(() => {}, ['hub'], {
          queue: 'dlx-consumer',
        })
        .spread(consumer => consumer.close())
    ));

    it('publish message and receive DLX response', () => (
      // it will be published to the `dlx-consumer` queue
      // and after 2250 ms moved to '' with routing key based on the
      // headers values
      this.dlx
        .publishAndWait('hub', { wont: 'be-consumed-queue' }, {
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
        const { toMiliseconds } = latency;

        assert.equal(toMiliseconds(initial.time), toMiliseconds(cached.time));
        assert(toMiliseconds(initial.time) < toMiliseconds(nonCached.time));
      });
    });

    after('close published', () => (
      this.cached.close()
    ));
  });

  describe('contentEncoding, contentType', () => {
    const gzip = Promise.promisify(require('zlib').gzip);
    let transport;

    before('init publisher', async () => {
      transport = await AMQPTransport.connect(configuration);
    });

    it('parses application/json+gzip', async () => {
      let response;
      const original = {
        sample: true,
        buf: Buffer.from('content'),
      };

      // send pre-serialized datum with gzip
      response = await transport.publishAndWait(
        'test.default',
        await gzip(JSON.stringify(original)),
        {
          skipSerialize: true,
          contentEncoding: 'gzip',
        }
      );
      assert.deepStrictEqual(response.resp, original);

      // pre-serialize no-gzip
      response = await transport.publishAndWait(
        'test.default',
        Buffer.from(JSON.stringify(original)),
        { skipSerialize: true }
      );
      assert.deepStrictEqual(response.resp, original);

      // not-serialized
      response = await transport.publishAndWait('test.default', original);
      assert.deepStrictEqual(response.resp, original);

      // not-serialized + gzip
      response = await transport.publishAndWait('test.default', original, { gzip: true });
      assert.deepStrictEqual(response.resp, original);
    });

    after('close publisher', () => transport.close());
  });

  describe('AMQPTransport.multiConnect', () => {
    let acksCalled = 0;
    const preCount = sinon.spy();
    const postCount = sinon.spy();

    const conf = {
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

      // adds QoS for the first queue, but not all the others
      const consumer = AMQPTransport.multiConnect(conf, spy, [{
        neck: 1,
      }]);

      const publisher = AMQPTransport.connect(configuration);

      return Promise.join(consumer, publisher, (multi, amqp) => {
        this.multi = multi;
        this.publisher = amqp;

        this.multi.on('pre', preCount);
        this.multi.on('after', postCount);
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

      return Promise
        .map(pub, message => (
          this.publisher.publishAndWait(message.route, message.message)
        ))
        .delay(10) // to allow async action to call 'after'
        .then((responses) => {
          assert.equal(acksCalled, q1.length);

          // ensure all responses match
          pub.forEach((p, idx) => {
            assert.equal(responses[idx], p.message);
          });

          assert.equal(this.spy.callCount, pub.length);

          // ensure that pre & after are called for each message
          assert.equal(preCount.callCount, pub.length);
          assert.equal(postCount.callCount, pub.length);
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
      return this.priority
        .createQueue({
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

  describe('double bind', function test() {
    before('init transport', () => {
      this.spy = sinon.spy(function responder(message, properties, actions, next) {
        next(null, { message, properties });
      });

      const opts = {
        connection: {
          ...configuration.connection,
          port: 5672,
          heartbeat: 2000,
        },
        exchange: 'test-topic',
        exchangeArgs: {
          autoDelete: false,
          type: 'topic',
        },
        defaultQueueOpts: {
          autoDelete: false,
          exclusive: false,
        },
        queue: 'nom-nom',
        bindPersistantQueueToHeadersExchange: true,
        listen: ['direct-binding-key', 'test.mandatory'],
      };

      return AMQPTransport
        .connect(opts, this.spy)
        .then((transport) => {
          this.transport = transport;
        });
    });

    it('delivers messages using headers', () => {
      return Promise
        .map(['direct-binding-key', 'doesnt-exist'], routingKey => (
          this.transport
            .publish('', 'hi', {
              confirm: true,
              exchange: 'amq.match',
              headers: {
                'routing-key': routingKey,
              },
            })
            .reflect()
        ))
        .delay(100)
        .then(() => {
          assert.ok(this.spy.calledOnce);
        });
    });

    after('close transport', () => {
      this.transport.close();
    });
  });

  describe('consumed queue', function test() {
    const tracer = new MockTracer();

    before('init transport', () => {
      this.proxy = new Proxy(9010, RABBITMQ_PORT, RABBITMQ_HOST);
      this.transport = new AMQPTransport({
        connection: {
          port: 9010,
          heartbeat: 2000,
        },
        debug: true,
        exchange: 'test-direct',
        exchangeArgs: {
          autoDelete: false,
          type: 'direct',
        },
        defaultQueueOpts: {
          autoDelete: true,
          exclusive: true,
        },
        tracer,
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
      const { transport } = this;
      const sample = { foo: 'bar' };
      const publish = () => transport.publishAndWait('/', sample, { confirm: true });

      let counter = 0;
      const args = [];
      transport.on('publish', (route, msg) => {
        if (route === '/') {
          args.push(msg);
          counter += 1;
        } else {
          counter += 1;
        }
      });

      return transport
        .createConsumedQueue(router, ['/'])
        .tap(() => Promise.all([
          publish(),
          Promise.delay(250).then(publish),
          Promise.delay(5000).then(publish),
          Promise.delay(300).then(() => this.proxy.interrupt(3000)),
        ]))
        .spread((consumer, queue, establishConsumer) => Promise.join(
          transport.stopConsumedQueue(establishConsumer),
          Promise.fromCallback(next => queue.delete(next))
        ))
        .finally(() => {
          transport.removeAllListeners('publish');
          assert.equal(counter, 6); // 3 requests, 3 responses
          for (const msg of args) {
            assert.deepStrictEqual(msg, sample);
          }
        });
    });

    it('should create consumed queue', (done) => {
      const { transport } = this;
      transport.on('consumed-queue-reconnected', (consumer, queue) => {
        debug('initial reconnect');
        // #2 reconnected, try publish
        return transport
          .publishAndWait('/', { foo: 'bar' }, { timeout: 500 })
          .then((message) => {
            // #4 OK, try unbind
            assert.deepEqual(message, { bar: 'baz' });
            debug('unbind exchange from /', queue.queueOptions.name);
            return transport.unbindExchange(queue, '/');
          })
          .then(() => {
            // #5 unbinded, let's reconnect
            transport.removeAllListeners('consumed-queue-reconnected');
            transport.on('consumed-queue-reconnected', () => {
              debug('reconnected for the second time, publish must not succeed');
              // #7 reconnected again
              transport.publish('/', { bar: 'foo' });
              Promise.delay(1000).tap(done);
            });

            // #6 trigger error again
            setTimeout(() => {
              debug('called interrupt (2) in 20');
              this.proxy.interrupt(20);
            }, 10);
          })
          .catch((e) => {
            debug('error for publish', e);
          });
      });

      transport.createConsumedQueue(router)
        .spread((consumer, queue) => transport.bindExchange(queue, '/'))
        .tap(() => {
          // #1 trigger error
          setTimeout(() => {
            debug('called interrupt (1) in 20');
            this.proxy.interrupt(20);
          }, 10);
        });
    });

    afterEach('tracer report', () => {
      const report = tracer.report();

      // print report for visuals
      console.log(printReport(report));
      assert.equal(report.unfinishedSpans.length, 0);
      tracer.clear();
    });

    after('close transport', () => {
      this.proxy.close();
      this.transport.close();
    });
  });

  describe('response headers', function test() {
    const tracer = new MockTracer();

    before('init transport', () => {
      this.proxy = new Proxy(9010, RABBITMQ_PORT, RABBITMQ_HOST);
      this.transport = new AMQPTransport({
        connection: {
          port: 9010,
          heartbeat: 2000,
        },
        debug: true,
        exchange: 'test-direct',
        exchangeArgs: {
          autoDelete: false,
          type: 'direct',
        },
        defaultQueueOpts: {
          autoDelete: true,
          exclusive: true,
        },
        tracer,
      });

      return this.transport.connect();
    });

    function router(message, headers, raw, next) {
      const error = new Error('Error occured but at least you still have your headers');

      switch (headers.routingKey) {
        case '/include-headers':
          assert.deepEqual(message, { foo: 'bar' });

          return next(null, { bar: 'baz' });
        case '/return-custom-header':
          assert.deepEqual(message, { foo: 'bar' });

          raw.properties[kReplyHeaders] = { 'x-custom-header': 'custom-header-value' };

          return next(null, { bar: 'baz' });
        case '/return-headers-on-error':
          assert.deepEqual(message, { foo: 'bar' });

          raw.properties[kReplyHeaders] = { 'x-custom-header': 'error-but-i-dont-care' };

          return next(error, null);
        default:
          throw new Error();
      }
    }

    it('is able to return detailed response with headers', async () => {
      const { transport } = this;
      const sample = { foo: 'bar' };

      let counter = 0;
      const args = [];
      transport.on('publish', (route, msg) => {
        if (route === '/include-headers') {
          args.push(msg);
          counter += 1;
        } else {
          counter += 1;
        }
      });

      try {
        const [, queue, establishConsumer] = await transport.createConsumedQueue(router, ['/include-headers']);

        const response = await transport.publishAndWait('/include-headers', sample, {
          confirm: true,
          simpleResponse: false,
        });

        assert.deepEqual(
          response,
          {
            data: { bar: 'baz' },
            headers: { timeout: 10000 },
          }
        );

        await Promise.join(
          transport.stopConsumedQueue(establishConsumer),
          Promise.fromCallback(next => queue.delete(next))
        );
      } catch (e) {
        throw e;
      } finally {
        transport.removeAllListeners('publish');
        assert.equal(counter, 2); // 1 requests, 1 responses
        for (const msg of args) {
          assert.deepStrictEqual(msg, sample);
        }
      }
    });

    it('is able to set custom reply headers', async () => {
      const { transport } = this;
      const sample = { foo: 'bar' };

      let counter = 0;
      const args = [];
      transport.on('publish', (route, msg) => {
        if (route === '/return-custom-header') {
          args.push(msg);
          counter += 1;
        } else {
          counter += 1;
        }
      });

      try {
        const [, queue, establishConsumer] = await transport.createConsumedQueue(router, ['/return-custom-header']);

        const response = await transport.publishAndWait('/return-custom-header', sample, {
          confirm: true,
          simpleResponse: false,
        });

        assert.deepEqual(
          response,
          {
            data: { bar: 'baz' },
            headers: { 'x-custom-header': 'custom-header-value', timeout: 10000 },
          }
        );

        await Promise.join(
          transport.stopConsumedQueue(establishConsumer),
          Promise.fromCallback(next => queue.delete(next))
        );
      } catch (e) {
        throw e;
      } finally {
        transport.removeAllListeners('publish');
        assert.equal(counter, 2); // 1 requests, 1 responses
        for (const msg of args) {
          assert.deepStrictEqual(msg, sample);
        }
      }
    });

    it('is able to return headers with error response', async () => {
      const { transport } = this;
      const sample = { foo: 'bar' };

      let counter = 0;
      const args = [];
      transport.on('publish', (route, msg) => {
        if (route === '/return-headers-on-error') {
          args.push(msg);
          counter += 1;
        } else {
          counter += 1;
        }
      });

      try {
        const [, queue, establishConsumer] = await transport.createConsumedQueue(router, ['/return-headers-on-error']);

        try {
          await transport.publishAndWait('/return-headers-on-error', sample, {
            confirm: true,
            simpleResponse: false,
          });
        } catch (error) {
          // here I should expect headers
          assert.strictEqual('Error occured but at least you still have your headers', error.message);

          await Promise.join(
            transport.stopConsumedQueue(establishConsumer),
            Promise.fromCallback(next => queue.delete(next))
          );
        }
      } catch (e) {
        throw e;
      } finally {
        transport.removeAllListeners('publish');
        assert.equal(counter, 2); // 1 requests, 1 responses
        for (const msg of args) {
          assert.deepStrictEqual(msg, sample);
        }
      }
    });

    after('cleanup', () => (
      Promise.map(['amqp', 'amqp_consumer'], name => (
        this[name] && this[name].close()
      ))
    ));
  });
});
