import { SingleFlightGroup } from "./single-flight"
import type { SingleFlightOptions } from "./types"

type KeyFunction<TArgs extends unknown[]> = (...args: TArgs) => string
type AsyncFunction<TArgs extends unknown[], TReturn> = (
  ...args: TArgs
) => Promise<TReturn>

/**
 * Method decorator that deduplicates concurrent calls using SingleFlight.
 *
 * ```ts
 * class UserService {
 *   @SingleFlight((id: number) => `user:${id}`)
 *   async getUser(id: number) { ... }
 * }
 * ```
 */
export function SingleFlight<TArgs extends unknown[]>(
  keyFn: KeyFunction<TArgs>,
  options?: SingleFlightOptions,
) {
  const sf = new SingleFlightGroup(options)

  return function (
    _target: object,
    _propertyKey: string | symbol,
    descriptor: PropertyDescriptor,
  ) {
    const original = descriptor.value as AsyncFunction<TArgs, unknown>

    descriptor.value = function (this: unknown, ...args: TArgs) {
      const key = keyFn(...args)
      return sf.execute(key, () => original.apply(this, args))
    }

    return descriptor
  }
}

/**
 * Wraps a function with SingleFlight deduplication.
 *
 * ```ts
 * const getUser = withSingleFlight(
 *   fetchUser,
 *   (id) => `user:${id}`,
 * )
 * ```
 */
export function withSingleFlight<TArgs extends unknown[], TReturn>(
  fn: AsyncFunction<TArgs, TReturn>,
  keyFn: KeyFunction<TArgs>,
  options?: SingleFlightOptions,
): (...args: TArgs) => Promise<TReturn> {
  const sf = new SingleFlightGroup(options)

  return (...args: TArgs) => {
    const key = keyFn(...args)
    return sf.execute(key, () => fn(...args))
  }
}
