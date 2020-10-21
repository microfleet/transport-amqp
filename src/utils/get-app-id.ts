import pkg from '../../package.json'
import { hostname } from 'os'
import { AMQPTransport } from '../amqp-transport'

export interface AppID {
  utils_version: string
  name: string
  host: string
  pid: number
  version: string
}

export const getAppID = (it: AMQPTransport): AppID => {
  return {
    pid: process.pid,
    name: it.config.name,
    host: hostname(),
    version: it.config.version,
    utils_version: pkg.version,
  }
}
