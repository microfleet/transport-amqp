// error generator
module.exports = function generateErrorMessage(routing, timeout) {
  return `job timed out on routing ${routing} after ${timeout} ms`;
};
