const Joi = require('joi');

/**
 * Settings confirm to [policy: string] : settings schema
 * @constructor
 * @param  {Object} settings - Container for policies.
 * @param  {Object} settings.* - Container for policy settings.
 * @param  {number} settings.*.min - Min delay for attempt.
 * @param  {number} settings.*.max - Max delay for attempt.
 * @param  {number} settings.*.factor - Exponential factor.
 */
class Backoff {
  constructor(settings) {
    this.settings = Object.setPrototypeOf({ ...settings }, null);
  }

  get(policy, attempt = 0) {
    const { min, factor, max } = this.settings[policy];

    if (attempt === 0) return 0;
    if (attempt === 1) return min;

    // eslint-disable-next-line no-restricted-properties
    return Math.min(Math.round((Math.random() + 1) * min * Math.pow(factor, attempt - 1)), max);
  }
}

Backoff.schema = Joi.object({
  private: Joi.object({
    min: Joi.number().min(0)
      .description('min delay for attempt #1')
      .default(250),

    max: Joi.number().min(0)
      .description('max delay')
      .default(1000),

    factor: Joi.number().min(1)
      .description('exponential increase factor')
      .default(1.2),
  }).default(),

  consumed: Joi.object({
    min: Joi.number().min(0)
      .description('min delay for attempt #1')
      .default(500),

    max: Joi.number().min(0)
      .description('max delay')
      .default(5000),

    factor: Joi.number().min(1)
      .description('exponential increase factor')
      .default(1.2),
  }).default(),
});

module.exports = Backoff;
