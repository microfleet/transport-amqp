/* eslint-disable import/no-dynamic-require */
const Promise = require('bluebird');

// Promisify stuff
['Exchange', 'Queue', 'Connection', 'Consumer', 'Publisher'].forEach((name) => {
  Promise.promisifyAll(require(`@microfleet/amqp-coffee/bin/src/lib/${name}`).prototype);
});

module.exports = require('@microfleet/amqp-coffee');
