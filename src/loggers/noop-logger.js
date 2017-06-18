// quick noop-logger implementation
const noop = require('lodash/noop');
const logLevels = require('./levels');

const assignLevels = (prev, level) => {
  prev[level] = noop;
  return prev;
};

logLevels.reduce(assignLevels, module.exports);
