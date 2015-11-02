// This file is required in mocha.opts
// The only purpose of this file is to ensure
// the babel transpiler is activated prior to any
// test code, and using the same babel options

require('babel-core/register')({
  optional: [ 'es7.objectRestSpread', 'es7.classProperties', 'es7.decorators' ],
});
