const HLRU = require('hashlru');
const hash = require('object-hash');
const is = require('is');
const latency = require('./latency');

class Cache {
  constructor(size) {
    this.enabled = !!size;

    // if enabled - use it
    if (this.enabled) {
      this.cache = HLRU(size);
    }
  }

  get(message, maxAge) {
    if (this.enabled === false) {
      // eslint-disable-next-line no-console
      console.warn('tried to use disabled cache');
      return null;
    }

    if (is.number(maxAge) === false) {
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

  set(key, data) {
    if (this.enabled === false) {
      // eslint-disable-next-line no-console
      console.warn('tried to use disabled cache');
      return null;
    }

    // only use string keys
    if (typeof key !== 'string') {
      return null;
    }

    return this.cache.set(key, { maxAge: process.hrtime(), value: data });
  }
}

module.exports = Cache;
