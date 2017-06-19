class Backoff {
  /**
   * Settings confirm to [policy: string] : settings schema
   * @constructor
   * @param  {Object} settings - Container for policies.
   * @param  {Object} settings.* - Container for policy settings.
   * @param  {number} settings.*.min - Min delay for attempt.
   * @param  {number} settings.*.max - Max delay for attempt.
   * @param  {number} settings.*.factor - Exponential factor.
   */
  constructor(settings) {
    this.settings = settings;
  }

  get(policy, attempt = 0) {
    const { min, factor, max } = this.settings[policy];

    if (attempt === 0) return 0;
    if (attempt === 1) return min;

    return Math.min(Math.round((Math.random() + 1) * min * (factor ** (attempt - 1))), max);
  }
}

module.exports = Backoff;
