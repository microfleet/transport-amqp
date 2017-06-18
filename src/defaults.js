module.exports = {
  name: 'amqp',
  private: false,
  cache: 100,
  exchange: 'node-services',
  exchangeArgs: {
    autoDelete: false,
    type: 'topic',
  },

  /**
   * Dead Letter Exchange Settings
   * @type {Object}
   */
  dlx: {
    // whether to enable or not
    enabled: true,
    params: {
      // default direct exchange, which is already existent and all
      // queues are bound to it with the routing key matching queue name
      // (amq.default) - has no name, empty string
      exchange: 'amq.headers',
      type: 'headers',
      autoDelete: false,
    },
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
