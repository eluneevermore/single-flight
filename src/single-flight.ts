import { SingleFlightTimeoutError } from "./types"
import type { Channel, Result, SingleFlightOptions } from "./types"

export class SingleFlightGroup {
  private readonly inflight = new Map<string, Promise<unknown>>()
  private readonly channel?: Channel
  private readonly timeout?: number

  constructor(options?: SingleFlightOptions) {
    this.channel = options?.channel
    this.timeout = options?.timeout
  }

  /**
   * Execute `fn` under deduplication for `key`.
   *
   * - If the same key is already in-flight locally, returns the existing promise.
   * - If a Channel is configured and another process holds the lock,
   *   waits for that process's result instead of executing.
   * - Otherwise, executes `fn` and shares the result.
   */
  execute<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const existing = this.inflight.get(key)
    if (existing) return existing as Promise<T>

    const promise = this.run<T>(key, fn)
    this.inflight.set(key, promise)

    const cleanup = () => {
      this.inflight.delete(key)
    }
    promise.then(cleanup, cleanup)

    return promise
  }

  /**
   * Remove a key from the local in-flight map, allowing the next call
   * to execute a fresh invocation. Does not affect distributed locks.
   */
  forget(key: string): void {
    this.inflight.delete(key)
  }

  private async run<T>(key: string, fn: () => Promise<T>): Promise<T> {
    if (!this.channel) {
      return fn()
    }

    const sub = await this.channel.subscribe(key)

    let acquired: boolean
    try {
      acquired = await this.channel.acquire(key)
    } catch (error) {
      sub.unsubscribe()
      throw error
    }

    if (acquired) {
      sub.unsubscribe()
      let result: Result<T>
      try {
        const value = await fn()
        result = { ok: true, value }
      } catch (error) {
        result = { ok: false, error }
      }
      try {
        await this.channel.release(key, result)
      } catch {
        // Release failure should not hide the executor result from the local caller.
      }
      if (result.ok) return result.value
      throw result.error
    }

    const result = await this.waitForResult(key, sub)
    if (result.ok) return result.value as T
    throw result.error
  }

  private waitForResult(
    key: string,
    sub: { result: Promise<Result>; unsubscribe(): void },
  ): Promise<Result> {
    if (this.timeout == null) return sub.result

    return new Promise<Result>((resolve, reject) => {
      const timer = setTimeout(() => {
        sub.unsubscribe()
        reject(new SingleFlightTimeoutError(key, this.timeout!))
      }, this.timeout)

      sub.result.then(
        (result) => {
          clearTimeout(timer)
          resolve(result)
        },
        (error) => {
          clearTimeout(timer)
          reject(error)
        },
      )
    })
  }

  async dispose(): Promise<void> {
    await this.channel?.dispose?.()
  }
}
