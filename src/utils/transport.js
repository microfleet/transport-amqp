const Promise = require('bluebird');

// Promisify stuff
['Exchange', 'Queue', 'Connection', 'Consumer', 'Publisher'].forEach((name) => {
  const path = require.resolve(`@microfleet/amqp-coffee/bin/src/lib/${name}`);
  /* eslint-disable import/no-dynamic-require */
  Promise.promisifyAll(require(path).prototype);
  /* eslint-enable import/no-dynamic-require */
});

const amqp = require('@microfleet/amqp-coffee');

amqp.prototype.consumeAsync = async function consumeAsync(...args) {
  let consumer;

  await Promise.fromCallback((next) => {
    consumer = this.consume(...args, next);
  });

  return consumer;
};


module.exports = amqp;
