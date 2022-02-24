/**
 * @typedef { import('pino').BaseLogger } Logger
 */

/**
 * @type {string[]}
 */
exports.levels = ['trace', 'debug', 'info', 'warn', 'error', 'fatal'];

/**
 *
 * @param {unknown} obj
 * @returns {obj is Logger}
 */
exports.isCompatible = (obj) => {
  return obj !== null
    && typeof obj === 'object'
    && exports.levels.every((level) => typeof obj[level] === 'function');
};

/**
 * @param {*} config
 * @returns {Logger}
 */
exports.prepareLogger = (config) => {
  // bunyan logger
  if (config.debug && !config.log) {
    try {
      return require('./pino-logger')(config.name);
    } catch (e) {
      return require('./noop-logger');
    }
  } else if (exports.isCompatible(config.log)) {
    return config.log;
  }

  return require('./noop-logger');
};
