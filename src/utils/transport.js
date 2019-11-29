const Promise = require('bluebird');

const kPromisified = Symbol.for('@microfleet/amqp-promisified');

// Promisify stuff
['Exchange', 'Queue', 'Connection', 'Consumer', 'Publisher'].forEach((name) => {
  const path = require.resolve(`@microfleet/amqp-coffee/bin/src/lib/${name}`);
  const mod = require(path); // eslint-disable-line import/no-dynamic-require

  if (mod[kPromisified]) return;

  Promise.promisifyAll(mod.prototype);
  mod[kPromisified] = true;
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
