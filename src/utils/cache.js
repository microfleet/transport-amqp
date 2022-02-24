const HLRU = require('hashlru');
const hash = require('object-hash');
const latency = require('./latency');

class Cache {
  /**
   * @param {number} size
   */
  constructor(size) {
    this.enabled = !!size;

    // if enabled - use it
    if (this.enabled) {
      // @ts-expect-error invalid lib definition
      this.cache = HLRU(size);
    }
  }

  /**
   *
   * @param {any} message
   * @param {number} maxAge
   * @returns
   */
  get(message, maxAge) {
    if (this.enabled === false) {
      process.emitWarning('tried to use disabled cache', {
        code: 'MF_AMQP_CACHE_0001',
        detail: 'enable cache to be able to use it',
      });
      return null;
    }

    if (typeof maxAge !== 'number' && maxAge > 0) {
      process.emitWarning('maxAge must be ', {
        code: 'MF_AMQP_CACHE_0002',
        detail: 'ensure maxAge is set to a positive number',
      });
      return null;
    }

    const hashKey = hash(message);
    const response = this.cache.get(hashKey);

    if (response !== undefined) {
      if (latency(response.maxAge) < maxAge) {
        return response;
      }

      this.cache.remove(hashKey);
    }

    return hashKey;
  }

  /**
   *
   * @param {string} key
   * @param {any} data
   * @returns
   */
  set(key, data) {
    if (this.enabled === false) {
      process.emitWarning('tried to use disabled cache', {
        code: 'MF_AMQP_CACHE_0001',
        detail: 'enable cache to be able to use it',
      });
      return null;
    }

    // only use string keys
    if (typeof key !== 'string') {
      process.emitWarning('key isnt string', {
        code: 'MF_AMQP_CACHE_0003',
      });
      return null;
    }

    return this.cache.set(key, { maxAge: process.hrtime(), value: data });
  }
}

module.exports = Cache;
