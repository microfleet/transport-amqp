function toMiliseconds(hrtime) {
  return (hrtime[0] * 1e3) + (hrtime[1] / 1e6).toFixed(3).valueOf();
}

module.exports = function latency(time) {
  return toMiliseconds(process.hrtime(time));
};

module.exports.toMiliseconds = toMiliseconds;
