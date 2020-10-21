import is from '@sindresorhus/is'
import hashlru from 'hashlru'
import hash from 'object-hash'
import assert from 'assert'

import { Schema } from '../schema'
import { LoggerLike } from '../schema/logger-like'
const latency = require('./latency')

type HashLRU = ReturnType<typeof hashlru>

export interface CacheOpts {
  size: Schema['cache']
  log: LoggerLike
}

export class Cache {
  public enabled: boolean
  private log: LoggerLike
  private cache: HashLRU = hashlru(0)

  constructor({ size, log }: CacheOpts) {
    this.log = log
    this.enabled = !!size

    // if enabled - use it
    if (this.enabled) {
      this.cache = hashlru(size)
    }
  }

  // TODO Message interface
  public get(message: any, maxAge: number) {
    try {
      assert(this.enabled, 'tried to use disabled cache')
      return this.#get(message, maxAge)
    } catch (e) {
      this.log.debug(e.message)
      return null
    }
  }

  public set(key: string, data: any) {
    try {
      assert(this.enabled, 'tried to use disabled cache')
      return this.#set(key, data)
    } catch (e) {
      this.log.debug(e.message)
      return null
    }
  }

  #get = (message: any, maxAge: number) => {
    if (is.number(maxAge) === false) {
      return null
    }

    const hashKey = hash(message)
    const response = this.cache.get(hashKey)

    if (response !== undefined) {
      if (latency(response.maxAge) < maxAge) {
        return response;
      }

      this.cache.remove(hashKey)
    }

    return hashKey
  }

  #set = (key: string, data: any) => {
    return this.cache.set(key, {
      maxAge: process.hrtime(),
      value: data,
    })
  }
}
