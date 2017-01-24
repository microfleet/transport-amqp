/* eslint-disable no-console, max-len, promise/always-return */

const chai = require('chai');
const Errors = require('common-errors');
const Promise = require('bluebird');
const Proxy = require('amqp-coffee/test/proxy').route;
const ld = require('lodash');
const stringify = require('json-stringify-safe');

const expect = chai.expect;

describe('AMQPTransport', function AMQPTransportTestSuite() {
  // require module
  const AMQPTransport = require('../src');
  const { jsonSerializer, jsonDeserializer } = require('../src/utils/serialization.js');
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
    expect(this.msg).to.be.eq('{"meta":{"controlsData":[0.25531813502311707,0.0011256206780672073,0.06426551938056946,-0.001104108989238739,0.852259635925293,0.005791602656245232,-0.5230863690376282,0,0.9999388456344604,0.011071242392063141,0.523118257522583,-0.009435615502297878,0.8522077798843384,0.8522599935531616,0,0.5231184363365173,0,0.005791574250906706,0.9999387264251709,-0.009435582906007767,0,-0.5230863690376282,0.011071248911321163,0.8522077798843384,0,-0.13242781162261963,0.06709221005439758,0.21647998690605164,1],"name":"oki-dokie"},"body":{"random":true,"data":[{"filename":"ok","version":10.3}]},"buffer":{"type":"Buffer","data":[120,120,120]}}');
  });

  it('deserializes message correctly', () => {
    expect(JSON.parse(this.msg, jsonDeserializer)).to.be.deep.eq(this.originalMsg);
  });

  it('serializes & deserializes error', () => {
    const serialized = stringify(new Error('ok'), jsonSerializer);
    const err = JSON.parse(serialized, jsonDeserializer);

    expect(err.name).to.be.eq('MSError');
    expect(!!err.stack).to.be.eq(true);
    expect(err.message).to.be.eq('ok');
  });

  it('is able to be initialized', () => {
    const amqp = new AMQPTransport(configuration);
    expect(amqp).to.be.an.instanceof(AMQPTransport);
    expect(amqp).to.have.ownProperty('_config');
    expect(amqp).to.have.ownProperty('_replyQueue');
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

    expect(createTransport).to.throw(Errors.ValidationError);
  });

  it('is able to connect to rabbitmq', () => {
    const amqp = this.amqp = new AMQPTransport(configuration);
    return amqp.connect()
      .then(() => {
        expect(amqp._amqp.state).to.be.eq('open');
      });
  });

  it('is able to disconnect', () => (
    this.amqp.close().then(() => {
      expect(this.amqp._amqp).to.be.eq(null);
    })
  ));

  it('is able to connect via helper function', () => (
    AMQPTransport
      .connect(configuration)
      .then((amqp) => {
        expect(amqp._amqp.state).to.be.eq('open');
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
        expect(amqp._amqp.state).to.be.eq('open');
        this.amqp_consumer = amqp;
      });
  });

  it('is able to publish to route consumer', () => (
    this.amqp
      .publishAndWait('test.default', 'test-message')
      .then((response) => {
        expect(response.resp).to.be.eq('test-message-response');
      })
  ));

  it('is able to publish to route consumer:2', () => (
    this.amqp
      .publishAndWait('test.default', 'test-message')
      .then((response) => {
        expect(response.resp).to.be.eq('test-message-response');
      })
  ));

  it('is able to publish to route consumer:2', () => (
    this.amqp
      .publishAndWait('test.default', 'test-message')
      .then((response) => {
        expect(response.resp).to.be.eq('test-message-response');
      })
  ));

  it('is able to send messages directly to a queue', () => {
    const privateQueue = this.amqp._replyTo;
    return this.amqp_consumer
      .sendAndWait(privateQueue, 'test-message-direct-queue')
      .reflect()
      .then((promise) => {
        expect(promise.isRejected()).to.be.eq(true);
        expect(promise.reason().name).to.be.eq('NotPermittedError');
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

  describe('cached request', () => {
    before('init consumer', () => {
      const transport = this.cached = new AMQPTransport(configuration);
      return transport.connect();
    });

    it('publishes batches of messages, they must return cached values and then new ones', () => {
      const transport = this.cached;
      const publish = () => transport.publishAndWait('test.default', 1, { cache: 500 });
      const promises = [
        publish(),
        Promise.delay(300).then(publish),
        Promise.delay(600).then(publish),
      ];

      return Promise.all(promises).spread((initial, cached, nonCached) => {
        const toMiliseconds = latency.toMiliseconds;
        expect(toMiliseconds(initial.time)).to.be.equal(toMiliseconds(cached.time));
        expect(toMiliseconds(initial.time)).to.be.lt(toMiliseconds(nonCached.time));
      });
    });

    after('close published', () => (
      this.cached.close()
    ));
  });

  describe('consumed queue', function test() {
    before('init transport', () => {
      this.proxy = new Proxy(9010, 5672, 'localhost');
      this.transport = new AMQPTransport({
        debug: true,
        connection: {
          port: 9010,
        },
        exchange: 'test-direct',
        exchangeArgs: {
          autoDelete: true,
          type: 'direct',
        },
        defaultQueueOpts: {
          autoDelete: true,
          exclusive: true,
        },
      });

      return this.transport.connect();
    });

    it('should create consumed queue', (done) => {
      const transport = this.transport;
      transport.on('consumed-queue-reconnected', (consumer, queue) => (
        // #2 reconnected, try publish
        transport
          .publishAndWait('/', { foo: 'bar' })
          .then((message) => {
            // #4 OK, try unbind
            expect(message).to.be.deep.eq({ bar: 'baz' });
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
      ));

      function router(message, headers, actions, next) {
        switch (headers.routingKey) {
          case '/':
            // #3 all right, try answer
            expect(message).to.be.deep.eq({ foo: 'bar' });
            return next(null, { bar: 'baz' });
          default:
            throw new Error();
        }
      }

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
