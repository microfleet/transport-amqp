'use strict';

const Promise = require('bluebird');
const Benchmark = require('benchmark');
const AMQPTransport = require('../lib');
const fmt = require('util').format;
const configuration = {
  exchange: 'test-exchange',
  connection: {
    host: process.env.RABBITMQ_PORT_5672_TCP_ADDR || 'localhost',
    port: process.env.RABBITMQ_PORT_5672_TCP_PORT || 5672,
  },
};

// simple back-forth
function listener(message, headers, actions, callback) {
  callback(null, 'ok');
}

// opts for consumer
const opts = Object.assign({}, configuration, {
  queue: 'test-queue',
  listen: 'test.default',
});

// publisher
const publisher = new AMQPTransport(configuration);
let messagesSent = 0;

Promise.join(
  AMQPTransport.connect(opts, listener),
  publisher.connect()
)
.spread(consumer => {
  const suite = new Benchmark.Suite('RabbitMQ');
  suite.add('Round-trip', {
    defer: true,
    fn: function test(deferred) {
      return publisher
        .publishAndWait('test.default', 'test-message')
        .then(() => {
          messagesSent++;
          deferred.resolve();
        });
    },
  })
  .on('complete', function suiteCompleted() {
    const stats = this.filter('fastest')[0].stats;
    const times = this.filter('fastest')[0].times;
    process.stdout.write(fmt('Messages sent: %s\n', messagesSent));
    process.stdout.write(fmt('Mean is', stats.mean * 1000 + 'ms', '~' + stats.rme + '%\n'));
    process.stdout.write(fmt('Total time is', times.elapsed + 's', times.period + 's\n'));
    consumer.close();
    publisher.close();
  })
  .run({ async: false, defer: true });
});
