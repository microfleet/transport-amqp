// quick noop-logger implementation
import noop from 'lodash/noop'
import { LoggerLike } from '../schema/logger-like'

export const noopLogger = () => {
  const logger: LoggerLike = Object.create(null)

  for (const level of Object.keys(LoggerLike.shape) as Array<keyof LoggerLike>) {
    logger[level] = noop
  }

  return logger
}

export default noopLogger
