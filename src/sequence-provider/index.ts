import hyperid from 'hyperid'

export class SequenceProvider {
  readonly #instance: hyperid.Instance

  constructor(opts: hyperid.Options = {}) {
    this.#instance = hyperid(opts)
  }

  next() {
    return this.#instance()
  }
}
