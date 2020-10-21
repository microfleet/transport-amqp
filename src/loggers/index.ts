import { Schema } from '../schema'
import { LoggerLike } from '../schema/logger-like'

let kInstance: LoggerLike | null = null

const prepareLogger = (config: Schema): LoggerLike => {
  if (config.log) {
    return config.log
  }

  if (config.debug) {
    try {
      return require('./pino-logger')(config.name)
    } catch (e) {
      return require('./noop-logger')
    }
  }

  return require('./noop-logger')
}

export const getInstance = (config?: Schema): LoggerLike => {
  if (kInstance) {
    return kInstance
  }

  if (!config) {
    throw new Error('No instance of logger is defined')
  }

  kInstance = prepareLogger(config)
  return kInstance
}
