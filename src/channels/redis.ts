import type { Redis } from "ioredis"
import type { Channel, Result, Subscription } from "../types"

const DEFAULT_PREFIX = "sf:"
const DEFAULT_LOCK_TTL_SECONDS = 30

export interface RedisChannelOptions {
  /** Key prefix for locks and pub/sub channels. Default: `"sf:"` */
  prefix?: string
  /** Lock TTL in seconds. Prevents deadlocks if executor crashes. Default: `30` */
  lockTTL?: number
  /**
   * A separate Redis connection for subscribing.
   * If not provided, the main connection is `.duplicate()`-d automatically.
   * Must not be shared with other command-issuing code because ioredis enters
   * subscriber mode and rejects regular commands on this connection.
   */
  subscriber?: Redis
}

export class RedisChannel implements Channel {
  private readonly redis: Redis
  private readonly subscriber: Redis
  private readonly ownsSubscriber: boolean
  private readonly prefix: string
  private readonly lockTTL: number
  private readonly listeners = new Map<string, Set<(result: Result) => void>>()

  constructor(redis: Redis, options?: RedisChannelOptions) {
    this.redis = redis
    this.prefix = options?.prefix ?? DEFAULT_PREFIX
    this.lockTTL = options?.lockTTL ?? DEFAULT_LOCK_TTL_SECONDS

    if (options?.subscriber) {
      this.subscriber = options.subscriber
      this.ownsSubscriber = false
    } else {
      this.subscriber = redis.duplicate()
      this.ownsSubscriber = true
    }

    this.subscriber.on("message", this.onMessage)
  }

  async subscribe(key: string): Promise<Subscription> {
    const channelName = this.channelName(key)
    let resolver!: (result: Result) => void
    let resolved = false

    const result = new Promise<Result>((resolve) => {
      resolver = (value: Result) => {
        if (resolved) return
        resolved = true
        resolve(value)
      }
    })

    let listeners = this.listeners.get(channelName)
    const needsSubscribe = !listeners
    if (!listeners) {
      listeners = new Set()
      this.listeners.set(channelName, listeners)
    }
    listeners.add(resolver)

    if (needsSubscribe) {
      await this.subscriber.subscribe(channelName)
    }

    return {
      result,
      unsubscribe: () => {
        if (resolved) return
        const currentListeners = this.listeners.get(channelName)
        if (!currentListeners) return

        currentListeners.delete(resolver)
        if (currentListeners.size === 0) {
          this.listeners.delete(channelName)
          void this.subscriber.unsubscribe(channelName)
        }
      },
    }
  }

  async acquire(key: string): Promise<boolean> {
    const result = await this.redis.set(
      this.lockName(key),
      "1",
      "EX",
      this.lockTTL,
      "NX",
    )
    return result === "OK"
  }

  async release(key: string, result: Result): Promise<void> {
    const message = serialize(result)
    await this.redis
      .multi()
      .publish(this.channelName(key), message)
      .del(this.lockName(key))
      .exec()
  }

  async dispose(): Promise<void> {
    this.subscriber.off("message", this.onMessage)
    if (this.ownsSubscriber) {
      this.subscriber.disconnect()
    }
  }

  private channelName(key: string): string {
    return `${this.prefix}ch:${key}`
  }

  private lockName(key: string): string {
    return `${this.prefix}lock:${key}`
  }

  private onMessage = (channel: string, message: string) => {
    const listeners = this.listeners.get(channel)
    if (!listeners) return

    let result: Result
    try {
      result = deserialize(message)
    } catch {
      result = {
        ok: false,
        error: new Error(
          `Failed to deserialize SingleFlight result on channel "${channel}": ${message}`,
        ),
      }
    }

    for (const listener of listeners) {
      listener(result)
    }

    this.listeners.delete(channel)
    void this.subscriber.unsubscribe(channel)
  }
}

function serialize(result: Result): string {
  if (result.ok) {
    return JSON.stringify({ ok: true, value: result.value })
  }

  const error = result.error
  return JSON.stringify({
    ok: false,
    error: {
      message: error instanceof Error ? error.message : String(error),
      name: error instanceof Error ? error.name : "Error",
      stack: error instanceof Error ? error.stack : undefined,
    },
  })
}

function deserialize(raw: string): Result {
  const parsed = JSON.parse(raw)
  if (parsed.ok) {
    return { ok: true, value: parsed.value }
  }

  const error = new Error(parsed.error.message)
  error.name = parsed.error.name
  if (parsed.error.stack) {
    error.stack = parsed.error.stack
  }
  return { ok: false, error }
}
