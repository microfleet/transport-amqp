import is from '@sindresorhus/is'
import { AnyFunction } from '../types'

interface Collection<Key, Value> {
  get(key: Key): Value | undefined
  set(key: Key, value: Value): this
  delete(key: Key): boolean
  values?: () => IterableIterator<Value>
}

interface CollectionConstructor {
  new <K extends object = object, V = any>(entries?: readonly [K, V][] | null): Collection<K, V>;
}

export class EntityStore<
  Type extends {},
> {
  #store: Collection<AnyFunction, Type>
  #StoreType: CollectionConstructor

  constructor(Store: CollectionConstructor = WeakMap) {
    this.#store = new Store()
    this.#StoreType = Store
  }

  get(handler: AnyFunction) {
    return this.#store.get(handler)
  }

  store(handler: AnyFunction, value: Type) {
    this.#store.set(handler, value)
  }

  forget(handler: AnyFunction){
    this.#store.delete(handler)
  }

  values() {
    if (is.function_(this.#store.values)) {
      return this.#store.values()
    }

    throw new Error(`The store of ${this.#StoreType} type doesn't support .values()`)
  }
}

