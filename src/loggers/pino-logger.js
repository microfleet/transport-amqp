const { pino } = require('pino');

// holy crap with stdout and good
const isProduction = process.env.NODE_ENV === 'production';

module.exports = (name = '@microfleet/transport-amqp', settings = {}) => {
  const opts = {
    name,
    level: isProduction ? 'info' : 'trace',
    ...settings,
  };

  return pino(opts, pino.destination(process.stdout.fd));
};
