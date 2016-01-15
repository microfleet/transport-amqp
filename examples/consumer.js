const AMQPTransport = require('../src');

const configuration = {
  exchange: 'test-exchange-not-durable',
  queue: 'vasya',
  listen: ['vasya.*'],
  exchangeArgs: {
    autoDelete: true,
  },
  connection: {
    host: process.env.RABBITMQ_PORT_5672_TCP_ADDR || 'localhost',
    port: process.env.RABBITMQ_PORT_5672_TCP_PORT || 5672,
  },
};

AMQPTransport.connect(configuration);
