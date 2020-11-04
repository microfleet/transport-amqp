import { BackoffOpts, BackoffPolicy } from '../schema/backoff'

/**
 * Settings confirm to [policy: string] : settings schema
 * @constructor
 * @param  {Object} settings - Container for policies.
 * @param  {Object} settings.* - Container for policy settings.
 * @param  {number} settings.*.min - Min delay for attempt.
 * @param  {number} settings.*.max - Max delay for attempt.
 * @param  {number} settings.*.factor - Exponential factor.
 */
export class Backoff {
  private readonly policies: BackoffOpts

  constructor(opts: BackoffOpts) {
    this.policies = Object.setPrototypeOf({ ...opts }, null);
  }

  get(policy: BackoffPolicy, attempt = 0) {
    const { min, factor, max } = this.policies[policy]

    if (attempt === 0) return 0
    if (attempt === 1) return min

    return Math.min(Math.round((Math.random() + 1) * min * Math.pow(factor, attempt - 1)), max)
  }
}

