module.exports = function latency(time) {
  const execTime = process.hrtime(time);
  return (execTime[0] * 1000) + (execTime[1] / 1000000).toFixed(3).valueOf();
};
