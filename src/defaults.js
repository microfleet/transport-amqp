module.exports = {
  name: 'amqp',
  private: false,
  exchange: 'node-services',
  exchangeArgs: {
    autoDelete: false,
    type: 'topic',
  },
  defaultOpts: {
    deliveryMode: 1,
    confirm: false,
    mandatory: false,
    immediate: false,
    headers: {},
  },
  defaultQueueOpts: {
    // specify for consumer queues
  },
  privateQueueOpts: {
    // specify for private queues
  },
  timeout: 10000,
  debug: process.env.NODE_ENV === 'development',
  connection: {
    host: 'localhost',
    port: 5672,
    login: 'guest',
    password: 'guest',
    vhost: '/',
    temporaryChannelTimeout: 6000,
    clientProperties: {
      capabilities: {
        consumer_cancel_notify: true,
      },
    },
  },
};
