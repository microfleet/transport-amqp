export type HRTime = ReturnType<typeof process.hrtime>

export function toMiliseconds(hrtime: HRTime) {
  return (hrtime[0] * 1e3) + (Math.round(hrtime[1] / 1e3) / 1e3)
}

export function latency(time: HRTime) {
  return toMiliseconds(process.hrtime(time))
}
