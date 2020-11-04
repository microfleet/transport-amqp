import { Tags, Span } from 'opentracing'

/**
 * set span on error
 * @param  {Span} span opentracing span
 * @param  {Promise} promise pending promise
 *
 * @return {Promise}
 */
export const wrapPromise = (
  span: Span,
  promise: Promise<any>
) => {
  try {
    return promise
  } catch (error) {
    span.setTag(Tags.ERROR, true)
    span.log({
      event: 'error',
      'error.object': error,
      message: error.message,
      stack: error.stack,
    })

    throw error
  } finally {
    span.finish()
  }
}
