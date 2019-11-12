const pino = require('pino');
const stdout = require('stdout-stream');

// holy crap with stdout and good
const isProduction = process.env.NODE_ENV === 'production';
const { write } = stdout;
stdout.write = (chunk, enc, next) => write.call(stdout, chunk, enc, next);

module.exports = (name = '@microfleet/transport-amqp', settings = {}) => {
  const opts = {
    name,
    level: isProduction ? 'info' : 'trace',
    serializers: {
      err: pino.stdSerializers.err,
    },
    ...settings,
  };

  return pino(opts, stdout);
};
