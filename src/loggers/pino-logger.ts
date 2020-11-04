const pino = require('pino')
const SonicBoom = require('sonic-boom')

// holy crap with stdout and good
const isProduction = process.env.NODE_ENV === 'production'

export const pinoLogger = (name = '@microfleet/transport-amqp', settings = {}) => {
  const opts = {
    name,
    level: isProduction ? 'info' : 'trace',
    ...settings,
  }

  // TODO some tools like flame tend to override stdout
  //      In such case, the fd might be `undefined`
  const boom = new SonicBoom({ fd: process.stdout.fd })

  return pino(opts, boom)
}

export default pinoLogger
