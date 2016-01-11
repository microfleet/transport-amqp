const chai = require('chai');
const expect = chai.expect;
const Errors = require('common-errors');
const Promise = require('bluebird');

describe('AMQPTransport', function AMQPTransportTestSuite() {
  // require module
  const AMQPTransport = require('../src');
  const configuration = { exchange: 'test-exchange', connection: {} };
  if (process.env.NODE_ENV === 'docker') {
    configuration.connection.host = process.env.RABBITMQ_PORT_5672_TCP_ADDR;
    configuration.connection.port = process.env.RABBITMQ_PORT_5672_TCP_PORT;
  }

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

  it('is able to disconnect', () => {
    return this.amqp.close().then(() => {
      expect(this.amqp._amqp).to.be.eq(null);
    });
  });

  it('is able to connect via helper function', () => {
    return AMQPTransport
      .connect(configuration)
      .then((amqp) => {
        expect(amqp._amqp.state).to.be.eq('open');
        this.amqp = amqp;
      });
  });

  it('is able to consume routes', () => {
    const opts = {
      exchange: configuration.exchange,
      queue: 'test-queue',
      listen: 'test.default',
      connection: configuration.connection,
    };

    return AMQPTransport
      .connect(opts, function listener(message, headers, actions, callback) {
        callback(null, message + '-response');
      })
      .then((amqp) => {
        expect(amqp._amqp.state).to.be.eq('open');
        this.amqp_consumer = amqp;
      });
  });

  it('is able to publish to route consumer', () => {
    return this.amqp.publishAndWait('test.default', 'test-message').then((response) => {
      expect(response).to.be.eq('test-message-response');
    });
  });

  it('is able to send messages directly to a queue', () => {
    const privateQueue = this.amqp._replyTo;
    return this.amqp_consumer.sendAndWait(privateQueue, 'test-message-direct-queue')
      .catch({ name: 'NotPermittedError' }, (err) => {
        expect(err.message).to.match(/no recipients found for message with correlation id/);
      });
  });

  describe('concurrent publish', () => {
    before('init consumer', () => {
      const transport = this.concurrent = new AMQPTransport(configuration);
      return transport.connect();
    });

    it('able to publish multiple messages at once', () => {
      const promises = [];
      const transport = this.concurrent;
      for (let i = 0; i < 5; i++) {
        promises.push(transport.publishAndWait('test.default', `ok.${i}`));
      }

      return Promise.all(promises);
    });

    after('close consumer', () => {
      return this.concurrent.close();
    });
  });

  after('cleanup', () => {
    return Promise.map(['amqp', 'amqp_consumer'], (name) => {
      return this[name] && this[name].close();
    });
  });
});
