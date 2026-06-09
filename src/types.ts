export type Result<T = unknown> =
  | { ok: true; value: T }
  | { ok: false; error: unknown }

export interface Subscription {
  readonly result: Promise<Result>
  unsubscribe(): void
}

/**
 * Channel handles cross-process/cross-machine coordination for SingleFlight.
 *
 * The protocol is: subscribe -> acquire -> release.
 * `subscribe` MUST be called before `acquire` to avoid missing results
 * published between the acquire check and the subscription setup.
 *
 * Implementations may use Redis pub/sub, process.send, unix sockets,
 * Kafka, BroadcastChannel, or any other IPC/messaging mechanism.
 */
export interface Channel {
  /**
   * Start listening for the result of key.
   * Must be called before `acquire()` to avoid race conditions.
   * Returns a Subscription whose `result` promise resolves when
   * the executor calls `release()`.
   */
  subscribe(key: string): Promise<Subscription>

  /**
   * Try to acquire exclusive execution rights for a key.
   * Returns `true` if this caller should execute the function.
   * Returns `false` if another process is already executing.
   */
  acquire(key: string): Promise<boolean>

  /**
   * Publish the result to all subscribers and release the lock.
   * Must be called by the executor after the function completes (success or error).
   */
  release(key: string, result: Result): Promise<void>

  /** Cleanup resources (connections, subscriptions, etc.) */
  dispose?(): void | Promise<void>
}

export interface SingleFlightOptions {
  channel?: Channel
  /**
   * Timeout in milliseconds for waiters (non-executors).
   * If the executor does not publish a result within this duration,
   * waiters reject with `SingleFlightTimeoutError` and clean up
   * their channel subscription to prevent memory leaks.
   *
   * Does not affect the executor. Use your own timeout on `fn()` for that.
   */
  timeout?: number
}

export class SingleFlightTimeoutError extends Error {
  constructor(
    public readonly key: string,
    public readonly timeoutMs: number,
  ) {
    super(`SingleFlight timed out waiting for key "${key}" after ${timeoutMs}ms`)
    this.name = "SingleFlightTimeoutError"
  }
}
