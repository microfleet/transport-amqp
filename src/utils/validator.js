const Validation = require('ms-validation');
const path = require('path');

/**
 * Schema filter
 */
function filter(filename) {
  return path.extname(filename) === '.json' && path.basename(filename, '.json') !== 'package';
}

module.exports = new Validation('../..', filter);
