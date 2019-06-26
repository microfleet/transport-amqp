const Joi = require('@hapi/joi');

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
  static schema = Joi.object({
    private: Joi.object({
      min: Joi.number().min(0)
        .default(250, 'min delay for attempt #1'),

      max: Joi.number().min(0)
        .default(1000, 'max delay'),

      factor: Joi.number().min(1)
        .default(1.2, 'exponential increase factor'),
    }).default(),

    consumed: Joi.object({
      min: Joi.number().min(0)
        .default(500, 'min delay for attempt #1'),

      max: Joi.number().min(0)
        .default(5000, 'max delay'),

      factor: Joi.number().min(1)
        .default(1.2, 'exponential increase factor'),
    }).default(),
  });

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

module.exports = Backoff;
