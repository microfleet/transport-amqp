import noop from 'lodash/noop'

import { AMQP } from '../utils/transport'
import { Consumer } from './consumer'

export class PrivateConsumer extends Consumer {
  async close() {
    const consumer = this.consumer
    consumer.removeAllListeners('error')
    consumer.removeAllListeners('cancel')
    consumer.on('error', noop)
    consumer.close()
  }

  protected initListeners = () => {
    const consumer = this.consumer

    // remove previous listener
    consumer.removeAllListeners('error')
    consumer.removeAllListeners('cancel')

    consumer.on('error', this.handleError)
    consumer.once('cancel', this.handleCancel)
  }

  protected handleCancel = (err: AMQP.ConsumerError, res?: any)  => {
    this.close()
    this.onCancelCallback(this, err, res)
  }

  protected onError = (err: AMQP.ConsumerError, res?: any) => {
    let shouldRecreate = false

    if (
      err
      && err.replyCode === 404
      && err.replyText.includes(this.boundQueue.name)
    ) {
      // https://github.com/dropbox/amqp-coffee#consumer-event-error
      // handle consumer error on reconnect and close consumer
      // warning: other queues (not private one) should be handled manually
      this.log.error('consumer returned 404 error', err)

      // reset replyTo queue and ignore all future errors
      this.close()
      shouldRecreate = true
    }

    this.onErrorCallback(this, shouldRecreate, err, res)
  }
}
