import Errors from 'common-errors'
import { MSError } from './serialization'

import type { LoggerLike } from '../schema/logger-like'
import type { AMQP } from './transport'

// error generator
export function generateErrorMessage(routing: string, timeout: any) {
  return `job timed out on routing ${routing} after ${timeout} ms`
};

export const AmqpDLXError = Errors.helpers.generateClass('AmqpDLXError', {
  args: ['xDeath', 'originalMessage'],

  // https://www.rabbitmq.com/dlx.html
  /**
   *  The dead-lettering process adds an array to the header of each dead-lettered message named x-death.
   *  This array contains an entry for each dead lettering event, identified by a pair of {queue, reason}.
   *  Each such entry is a table that consists of several fields:
   *    - queue - the name of the queue the message was in before it was dead-lettered,
   *    - reason - see below,
   *    - time - the date and time the message was dead lettered as a 64-bit AMQP format timestamp,
   *    - exchange - the exchange the message was published to (note that this will be a dead letter exchange if
   *      the message is dead lettered multiple times),
   *    - routing-keys - the routing keys (including CC keys but excluding  BCC ones) the message was published with,
   *    - count - how many times this message was dead-lettered in this queue for this reason, and
   *    - original-expiration (if the message was dead-letterered due to per-message TTL) - the original expiration
   *      property of the message. The expiration property is removed from the message on dead-lettering in order to
   *      prevent it from expiring again in any queues it is routed to.
   *
   *  New entries are prepended to the beginning of the x-death array. In case x-death already contains an entry
   *  with the same queue and dead lettering reason, its count field will be incremented and it will be moved to the beginning of the array.
   *
   *  The reason is a name describing why the message was dead-lettered and is one of the following:
   *    - rejected - the message was rejected with requeue=false,
   *    - expired - the TTL of the message expired; or
   *    - maxlen - the maximum allowed queue length was exceeded.
   *
   *    Note that the array is sorted most-recent-first, so the most recent dead-lettering will be recorded in the first entry.
   */
  generateMessage() {
    const message: string[] = []

    // TODO rejection entry type
    // @ts-expect-error
    this.xDeath.forEach((rejectionEntry) => {
      switch (rejectionEntry.reason) {
        case 'rejected':
          message.push(`Rejected from ${rejectionEntry.queue} ${rejectionEntry.count} time(s)`)
          break

        case 'expired':
          message.push(`Expired from queue "${rejectionEntry.queue}" with routing keys `
            + `${JSON.stringify(rejectionEntry['routing-keys'])} `
            + `after ${rejectionEntry['original-expiration']}ms ${rejectionEntry.count} time(s)`)
          break

        case 'maxlen':
          message.push(`Overflown ${rejectionEntry.queue} ${rejectionEntry.count} time(s)`)
          break

        default:
          message.push(`Unexpected DLX reason: ${rejectionEntry.reason}`)
      }
    })

    return message.join('. ')
  },
})

// error data that is going to be copied
const copyErrorData = [
  'code', 'name', 'errors',
  'field', 'reason', 'stack',
]

type ErrorLike = Record<string, any> & {
  message?: string
}

/**
 * Wraps response error
 * @param {Error} originalError
 * @returns {Error}
 */
export const wrapError = (originalError: Error | ErrorLike): Error => {
  if (originalError instanceof Error) {
    return originalError
  }

  // this only happens in case of .toJSON on error object
  const error = new MSError(originalError.message)

  for (const fieldName of copyErrorData) {
    const mixedData = originalError[fieldName]
    if (mixedData !== undefined && mixedData !== null) {
      // @ts-expect-error
      error[fieldName] = mixedData
    }
  }

  return error
}

export function on406(this: { log: LoggerLike }, params: any, err: AMQP.ConsumerError) {
  this.log.warn({ params }, '[406] error declaring exchange/queue:', err.replyText)
}
