const Errors = require('common-errors');

// error generator
module.exports = function generateErrorMessage(routing, timeout) {
  return `job timed out on routing ${routing} after ${timeout} ms`;
};

module.exports.AmqpDLXError = Errors.helpers.generateClass('AmqpDLXError', {
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
    const message = [];

    this.xDeath.forEach((rejectionEntry) => {
      switch (rejectionEntry.reason) {
        case 'rejected':
          message.push(`Rejected from ${rejectionEntry.queue} ${rejectionEntry.count} time(s)`);
          break;

        case 'expired':
          message.push(`Expired from queue "${rejectionEntry.queue}" with routing keys ` +
            `${JSON.stringify(rejectionEntry['routing-keys'])} ` +
            `after ${rejectionEntry['original-expiration']}ms ${rejectionEntry.count} time(s)`);
          break;

        case 'maxlen':
          message.push(`Overflown ${rejectionEntry.queue} ${rejectionEntry.count} time(s)`);
          break;

        default:
          message.push(`Unexpected DLX reason: ${rejectionEntry.reason}`);
      }
    });

    return message.join('. ');
  },
});
