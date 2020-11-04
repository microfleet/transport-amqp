import {
  ValidationError
} from 'common-errors'

export const kReplyHeaders
  = Symbol('headers')

export const kParseError
  = new ValidationError(`couldn't deserialize input`, '500', 'message.raw')

