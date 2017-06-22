const initTracer = require('jaeger-client').initTracer;
const AMQPTransport = require('../src');
const Promise = require('bluebird');

const reporter = {
  flushIntervalMs: 300,
  logSpans: false,
};

const sampler = {
  type: 'const',
  param: 1,
};

const opts = {
  logger: require('../lib/loggers/bunyan-logger'),
};

const connection = {
  host: process.env.RABBITMQ_PORT_5672_TCP_ADDR || 'localhost',
  port: process.env.RABBITMQ_PORT_5672_TCP_PORT || 5672,
};

const server = {
  connection: Object.assign({}, connection),
  name: 'server',
  queue: 'consumer',
  listen: ['tracing-key', 'pomegranate.*'],
  tracer: initTracer({ serviceName: 'rpc-server', reporter, sampler }, opts),
};

const client = {
  connection: Object.assign({}, connection),
  name: 'client',
  private: true,
  tracer: initTracer({ serviceName: 'rpc-client', reporter, sampler }, opts),
};

const echo = (message, properties, actions, next) => {
  if (!message) return next(new Error('invalid message'));
  if (message.err) return next(message.err);
  return next(null, message);
};

Promise.join(
  AMQPTransport.connect(server, echo),
  AMQPTransport.connect(client)
)
.spread((rpcServer, rpcClient) => Promise.all([
  rpcClient.publishAndWait('tracing-key', {}).reflect(),
  rpcClient.publishAndWait('pomegranate.turnKey', {}).reflect(),
  rpcClient.publishAndWait('pomegranate.turnKey').reflect(),
]));
