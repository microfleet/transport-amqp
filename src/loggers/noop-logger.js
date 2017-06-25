// quick noop-logger implementation
const noop = require('lodash/noop');
const logLevels = require('./index').levels;

const assignLevels = (prev, level) => {
  prev[level] = noop;
  return prev;
};

logLevels.reduce(assignLevels, module.exports);
