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

  it('is able to connect to rabbitmq', async () => {
    const amqp = this.amqp = new AMQPTransport(configuration);
    await amqp.connect();
    assert.equal(amqp._amqp.state, 'open');
  });

  it('is able to disconnect', async () => {
    await this.amqp.close();
    assert.equal(this.amqp._amqp, null);
  });

  it('is able to connect via helper function', async () => {
    const amqp = await AMQPTransport.connect(configuration);
    assert.equal(amqp._amqp.state, 'open');
    this.amqp = amqp;
  });

  it('is able to consume routes', async () => {
    const opts = {
      cache: 100,
      exchange: configuration.exchange,
      queue: 'test-queue',
      listen: 'test.default',
      connection: configuration.connection,
    };

    const amqp = await AMQPTransport.connect(opts, (message, headers, actions, callback) => {
      callback(null, {
        resp: typeof message === 'object' ? message : `${message}-response`,
        time: process.hrtime(),
      });
    });

    assert.equal(amqp._amqp.state, 'open');
    this.amqp_consumer = amqp;
  });

  after('cleanup', async () => (
    Promise.map(['amqp', 'amqp_consumer'], (name) => (
      this[name] && this[name].close()
    ))
  ));

  it('is able to publish to route consumer', async () => {
    const response = await this.amqp
      .publishAndWait('test.default', 'test-message');

    assert.equal(response.resp, 'test-message-response');
  });

  it('is able to publish to route consumer:2', async () => {
    const response = await this.amqp
      .publishAndWait('test.default', 'test-message');

    assert.equal(response.resp, 'test-message-response');
  });

  it('is able to publish to route consumer:2', async () => {
    const response = await this.amqp
      .publishAndWait('test.default', 'test-message');

    assert.equal(response.resp, 'test-message-response');
  });

  it('is able to send messages directly to a queue', async () => {
    const privateQueue = this.amqp._replyTo;
    const promise = await this.amqp_consumer
      .sendAndWait(privateQueue, 'test-message-direct-queue')
      .reflect();

    assert.equal(promise.isRejected(), true);
    assert.equal(promise.reason().name, 'NotPermittedError');
  });

  describe('concurrent publish', () => {
    before('init consumer', () => {
      const transport = this.concurrent = new AMQPTransport(configuration);
      return transport.connect();
    });

    it('able to publish multiple messages at once', () => {
      const transport = this.concurrent;
      const promises = ld.times(5, (i) => (
        transport.publishAndWait('test.default', `ok.${i}`)
      ));
      return Promise.all(promises);
    });

    after('close consumer', async () => {
      await this.concurrent.close();
    });
  });

  describe('DLX: enabled', () => {
    before('init amqp', () => {
      const transport = this.dlx = new AMQPTransport(configuration);
      return transport.connect();
    });

    after('close amqp', async () => {
      await this.dlx.close();
    });

    it('create queue, but do not consume', async () => {
      const kReestablishQueue = await this.dlx.createConsumedQueue(() => {}, ['hub'], {
        queue: 'dlx-consumer',
      });

      await this.dlx._consumers.get(kReestablishQueue).close();
    });

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
  });

  describe('cached request', () => {
    before('init consumer', () => {
      const transport = this.cached = new AMQPTransport(configuration);
      return transport.connect();
    });

    after('close published', async () => {
      await this.cached.close();
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
  });

  describe('contentEncoding, contentType', () => {
    const gzip = Promise.promisify(require('zlib').gzip);
    let transport;

    before('init publisher', async () => {
      transport = await AMQPTransport.connect(configuration);
    });

    after('close publisher', () => transport.close());

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

    after('close multi-transport', async () => {
      await this.multi.close();
      await this.publisher.close();
    });

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

    it('verify that messages are all received & acked', async () => {
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

      const responses = await Promise
        .map(pub, (message) => (
          this.publisher.publishAndWait(message.route, message.message)
        ))
        .delay(10); // to allow async action to call 'after'

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

    after('close priority-transport', async () => {
      await this.priority.close();
      await this.publisher.close();
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
  });

  describe('double bind', function test() {
    before('init transport', async () => {
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

      this.transport = await AMQPTransport.connect(opts, this.spy);
    });

    after('close transport', async () => {
      await this.transport.close();
    });

    it('delivers messages using headers', async () => {
      await Promise
        .map(['direct-binding-key', 'doesnt-exist'], (routingKey) => (
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
        .delay(100);

      assert.ok(this.spy.calledOnce);
    });
  });

  describe('Consumers externally available', function suite() {
    before('init transport', async () => {
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
      });

      const amqp = await this.transport.connect();
      sinon.spy(amqp, 'closeAllConsumers');
    });

    after('close transport', async () => {
      const { transport } = this;

      transport.closeAllConsumers.restore();
      await transport.close();
      await this.proxy.close();
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

    it('`consumers` map filled', async () => {
      const { transport } = this;

      await transport.createConsumedQueue(router, ['/']);

      const { _consumers } = transport;

      assert.strictEqual(_consumers.size, 1);
      await transport.closeAllConsumers();

      assert.equal(_consumers.size, 0);
    });
  });

  describe('consumed queue', function test() {
    const tracer = new MockTracer();

    before('init transport', async () => {
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

      await this.transport.connect();
    });

    afterEach('tracer report', () => {
      const report = tracer.report();

      // print report for visuals
      console.log(printReport(report)); // eslint-disable-line no-console
      assert.equal(report.unfinishedSpans.length, 0);
      tracer.clear();
    });

    after('close transport', async () => {
      await this.transport.close();
      await this.proxy.close();
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

    it('reestablishing consumed queue', async () => {
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

      try {
        const kReestablishConsumer = await transport.createConsumedQueue(router, ['/']);

        await Promise.all([
          publish(),
          Promise.delay(250).then(publish),
          Promise.delay(300).then(() => this.proxy.interrupt(3000)),
          Promise.delay(5000).then(publish),
        ]);

        await Promise.all([
          transport.closeAllConsumers(),
          transport._queues.get(kReestablishConsumer).deleteAsync(),
        ]);
      } finally {
        transport.removeAllListeners('publish');
        assert.equal(counter, 6); // 3 requests, 3 responses
        for (const msg of args) {
          assert.deepStrictEqual(msg, sample);
        }
      }
    });

    it('should create consumed queue', async () => {
      const { transport } = this;
      let done;
      let fail;
      const promise = new Promise((resolve, reject) => {
        done = resolve;
        fail = reject;
      });

      const kReestablishConsumer = await transport.createConsumedQueue(router);
      const queue = transport._queues.get(kReestablishConsumer);

      await transport.bindExchange(queue, '/');

      // #1 trigger error
      debug('called interrupt (1) in 20');
      this.proxy.interrupt(20);

      let attempt = 0;
      transport.on('consumed-queue-reconnected', async () => {
        attempt += 1;
        assert.equal(attempt, 1, 'must only trigger once');

        // #2 reconnected, try publish
        try {
          const message = await transport
            .publishAndWait('/', { foo: 'bar' }, { timeout: 500 });

          // #4 OK, try unbind
          assert.deepEqual(message, { bar: 'baz' });
          debug('unbind exchange from /', queue.queueOptions.name);

          const activeQueue = transport._queues.get(kReestablishConsumer);
          await transport.unbindExchange(activeQueue, '/');

          // #5 unbound, let's reconnect
          transport.removeAllListeners('consumed-queue-reconnected');
          transport.on('consumed-queue-reconnected', async () => {
            debug('reconnected for the second time, publish must not succeed');

            // #7 reconnected again
            // dont wait for actual publish, if message comes router
            // will throw and crash the process
            transport.publish('/', { bar: 'foo' });

            // resolve only on second attempt after proxy interrupt
            await Promise.delay(1000).then(done);
          });

          // #6 trigger error again
          await Promise.delay(10);
          debug('called interrupt (2) in 20');
          this.proxy.interrupt(20);
        } catch (e) {
          debug('error for publish', e);
          fail(e);
        }
      });

      await promise;
    });
  });

  describe('response headers', function test() {
    const tracer = new MockTracer();

    before('init transport', async () => {
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

      await this.transport.connect();
    });

    after('cleanup', async () => {
      await this.transport.close();
      await this.proxy.close();
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
        const kReestablishConsumer = await transport.createConsumedQueue(router, ['/include-headers']);

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

        await Promise.all([
          transport.closeAllConsumers(),
          transport._queues.get(kReestablishConsumer).deleteAsync(),
        ]);
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
        const kReestablishConsumer = await transport.createConsumedQueue(router, ['/return-custom-header']);

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

        await Promise.all([
          transport.closeAllConsumers(),
          transport._queues.get(kReestablishConsumer).deleteAsync(),
        ]);
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
        const kReestablishConsumer = await transport.createConsumedQueue(router, ['/return-headers-on-error']);

        try {
          await transport.publishAndWait('/return-headers-on-error', sample, {
            confirm: true,
            simpleResponse: false,
          });
        } catch (error) {
          // here I should expect headers
          assert.strictEqual('Error occured but at least you still have your headers', error.message);
          assert.deepEqual({ 'x-custom-header': 'error-but-i-dont-care', timeout: 10000 }, error[kReplyHeaders]);

          await Promise.all([
            transport.closeAllConsumers(),
            transport._queues.get(kReestablishConsumer).deleteAsync(),
          ]);
        }
      } finally {
        transport.removeAllListeners('publish');
        assert.equal(counter, 2); // 1 requests, 1 responses
        for (const msg of args) {
          assert.deepStrictEqual(msg, sample);
        }
      }
    });
  });
});
