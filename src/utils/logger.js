const bunyan = require('bunyan');
const stdout = require('stdout-stream');

// holy crap with stdout and good
const isProduction = process.env.NODE_ENV === 'production';
const write = stdout.write;
stdout.write = (chunk, enc, next) => write.call(stdout, chunk, enc, next);

const logger = bunyan.createLogger({
  name: 'ms-amqp-transport',
  src: !isProduction,
  serializers: bunyan.stdSerializers,
  streams: [
    {
      level: isProduction ? 'info' : 'trace',
      stream: stdout,
    },
  ],
});

module.exports = logger;
