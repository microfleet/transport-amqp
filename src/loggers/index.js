const every = require('lodash/every');
const is = require('is');

exports.levels = ['trace', 'debug', 'info', 'warn', 'error', 'fatal'];

exports.isCompatible = (obj) => {
  return obj !== null &&
    typeof obj === 'object' &&
    every(exports.levels, level => is.fn(obj[level]));
};

exports.prepareLogger = (config) => {
  // bunyan logger
  if (config.debug && !config.log) {
    try {
      return require('./bunyan-logger');
    } catch (e) {
      return require('./noop-logger');
    }
  } else if (exports.isCompatible(config.log)) {
    return config.log;
  }

  return require('./noop-logger');
};
