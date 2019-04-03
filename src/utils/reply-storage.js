const Errors = require('common-errors');
const generateErrorMessage = require('./error');

/**
 * In-memory reply storage
 */
class ReplyStorage {
  constructor(Type = Map) {
    this.storage = new Type();
    this.onTimeout = this.onTimeout.bind(this);
  }

  /**
   * Invoked on Timeout Error
   * @param  {string} correlationId
   * @returns {Void}
   */
  onTimeout(correlationId) {
    const { storage } = this;
    const { reject, routing, timeout } = storage.get(correlationId);

    // clean-up
    storage.delete(correlationId);

    // reject with a timeout error
    setImmediate(reject, new Errors.TimeoutError(generateErrorMessage(routing, timeout)));
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
  push(correlationId, opts) {
    opts.timer = setTimeout(this.onTimeout, opts.timeout, correlationId);
    this.storage.set(correlationId, opts);
  }

  /**
   * Rejects stored promise with an error & cleans up
   * Timeout error
   * @param  {string} correlationId
   * @param  {Error} error
   * @returns {void}
   */
  reject(correlationId, error) {
    const { storage } = this;
    const { timer, reject } = storage.get(correlationId);

    // remove timer
    clearTimeout(timer);

    // remove reference
    storage.delete(correlationId);

    // now resolve promise and return an error
    setImmediate(reject, error);
  }

  pop(correlationId) {
    const future = this.storage.get(correlationId);

    // if undefind - early return
    if (future === undefined) {
      return undefined;
    }

    // cleanup timeout
    clearTimeout(future.timer);

    // remove reference to it
    this.storage.delete(correlationId);

    // return data
    return future;
  }
}

module.exports = ReplyStorage;
