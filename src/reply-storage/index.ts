import { TimeoutError } from 'common-errors'
import { setTimeout } from 'timers'
import { ReplyOptions } from '../message-options'
import { generateErrorMessage } from '../utils/error'

export interface PendingReplyConf {
  // promise resolve fn
  resolve(value: any): void

  // promise reject fn
  reject(reason: any): void

  // expected response time
  timeout: number
  // routing key for error message.
  routing: string
  // process.hrtime() results
  time: ReturnType<typeof process['hrtime']>
  // cache key
  cache: string | null
  // reply options:
  replyOptions: ReplyOptions
  // whether return body-only response or include headers
  simple?: boolean
}

export interface PendingReply extends PendingReplyConf {
  // NodeJS timeout
  timer: NodeJS.Timeout
}

/**
 * In-memory reply storage
 */
export class ReplyStorage {
  private storage: Map<string, PendingReply>

  constructor(Type: MapConstructor = Map) {
    this.storage = new Type()
  }

  /**
   * Invoked on Timeout Error
   * @param  {string} correlationId
   * @returns {Void}
   */
  onTimeout = (correlationId: string) => {
    const { storage } = this
    const future = storage.get(correlationId)

    // if undefined - early return
    if (future === undefined) {
      return
    }

    const { reject, routing, timeout } = future

    // clean-up
    storage.delete(correlationId)

    // reject with a timeout error
    setImmediate(reject, new TimeoutError(generateErrorMessage(routing, timeout)))
  }

  /**
   * Stores correlation ID in the memory storage
   * @param  {string} correlationId
   * @param  {Object} opts - Container.
   * @param  {Function} opts.resolve -  promise resolve action.
   * @param  {Function} opts.reject - promise reject action.
   * @param  {number} opts.timeout - expected response time.
   * @param  {string} opts.routing - routing key for error message.
   * @param  {boolean} opts.simple - whether return body-only response or include headers
   * @param  {Array[number]} opts.time - process.hrtime() results.
   * @returns {Void}
   */
  push(correlationId: string, opts: PendingReplyConf) {
    (opts as PendingReply).timer = setTimeout(
      this.onTimeout,
      opts.timeout,
      correlationId,
    )

    this.storage.set(correlationId, opts as PendingReply)
  }

  /**
   * Rejects stored promise with an error & cleans up
   * Timeout error
   * @param  {string} correlationId
   * @param  {Error} error
   * @returns {void}
   */
  reject(correlationId: string, error: Error) {
    const { storage } = this
    const future = storage.get(correlationId)

    // if undefined - early return
    if (future === undefined) {
      return
    }

    const { timer, reject } = future

    // remove timer
    clearTimeout(timer)

    // remove reference
    storage.delete(correlationId)

    // now resolve promise and return an error
    setImmediate(reject, error)
  }

  pop(correlationId: string) {
    const future = this.storage.get(correlationId)

    // if undefined - early return
    if (future === undefined) {
      return undefined
    }

    // cleanup timeout
    clearTimeout(future.timer)

    // remove reference to it
    this.storage.delete(correlationId)

    // return data
    return future
  }
}
