/**
 * @param {[number, number]} hrtime
 * @returns
 */
function toMiliseconds(hrtime) {
  return (hrtime[0] * 1e3) + (Math.round(hrtime[1] / 1e3) / 1e3);
}

/**
 * @param {[number, number]} time
 * @returns
 */
module.exports = function latency(time) {
  return toMiliseconds(process.hrtime(time));
};

module.exports.toMiliseconds = toMiliseconds;
