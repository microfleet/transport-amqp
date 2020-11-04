export type HRTime = ReturnType<typeof process.hrtime>

export function toMilliseconds(hrtime: HRTime) {
  return (hrtime[0] * 1e3) + (Math.round(hrtime[1] / 1e3) / 1e3)
}

export function latency(time: HRTime) {
  return toMilliseconds(process.hrtime(time))
}
