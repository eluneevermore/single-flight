import { EventEmitter } from "node:events"
import { RedisChannel } from "../src/channels/redis"
import { SingleFlightGroup } from "../src/single-flight"

function createMockRedis() {
  const emitter = new EventEmitter()
  const subscribed = new Set<string>()
  let lastMulti:
    | {
        publish: ReturnType<typeof vi.fn>
        del: ReturnType<typeof vi.fn>
        exec: ReturnType<typeof vi.fn>
      }
    | undefined

  const mock = {
    on: emitter.on.bind(emitter),
    off: emitter.off.bind(emitter),
    emit: emitter.emit.bind(emitter),
    subscribe: vi.fn(async (channel: string) => {
      subscribed.add(channel)
    }),
    unsubscribe: vi.fn(async (channel: string) => {
      subscribed.delete(channel)
    }),
    set: vi.fn(async () => null as string | null),
    publish: vi.fn(async () => 0),
    del: vi.fn(async () => 0),
    multi: vi.fn(() => {
      lastMulti = {
        publish: vi.fn().mockReturnThis(),
        del: vi.fn().mockReturnThis(),
        exec: vi.fn(async () => []),
      }
      return lastMulti
    }),
    duplicate: vi.fn(() => createMockRedis()),
    disconnect: vi.fn(),
    getLastMulti() {
      return lastMulti
    },
    _subscribed: subscribed,
  }

  return mock
}

function createChannelPair(options?: { prefix?: string; lockTTL?: number }) {
  const redis = createMockRedis()
  const subscriber = createMockRedis()
  const channel = new RedisChannel(redis as never, {
    subscriber: subscriber as never,
    ...options,
  })

  return { redis, subscriber, channel }
}

describe("RedisChannel", () => {
  describe("subscribe", () => {
    it("calls SUBSCRIBE on the subscriber connection", async () => {
      const { subscriber, channel } = createChannelPair()

      await channel.subscribe("mykey")

      expect(subscriber.subscribe).toHaveBeenCalledWith("sf:ch:mykey")
    })

    it("reuses existing subscription for the same key", async () => {
      const { subscriber, channel } = createChannelPair()

      await channel.subscribe("k")
      await channel.subscribe("k")

      expect(subscriber.subscribe).toHaveBeenCalledTimes(1)
    })

    it("uses custom prefix", async () => {
      const { subscriber, channel } = createChannelPair({ prefix: "app:" })

      await channel.subscribe("x")

      expect(subscriber.subscribe).toHaveBeenCalledWith("app:ch:x")
    })

    it("unsubscribe removes listener and calls UNSUBSCRIBE when last", async () => {
      const { subscriber, channel } = createChannelPair()

      const sub1 = await channel.subscribe("k")
      const sub2 = await channel.subscribe("k")

      sub1.unsubscribe()
      expect(subscriber.unsubscribe).not.toHaveBeenCalled()

      sub2.unsubscribe()
      expect(subscriber.unsubscribe).toHaveBeenCalledWith("sf:ch:k")
    })

    it("unsubscribe is idempotent after result is received", async () => {
      const { subscriber, channel } = createChannelPair()

      const sub = await channel.subscribe("k")

      subscriber.emit(
        "message",
        "sf:ch:k",
        JSON.stringify({ ok: true, value: "v" }),
      )

      const result = await sub.result
      expect(result).toEqual({ ok: true, value: "v" })

      sub.unsubscribe()
      expect(subscriber.unsubscribe).toHaveBeenCalledTimes(1)
    })
  })

  describe("acquire", () => {
    it("calls SET with NX and EX", async () => {
      const { redis, channel } = createChannelPair()
      redis.set.mockResolvedValue(null)

      await channel.acquire("mykey")

      expect(redis.set).toHaveBeenCalledWith("sf:lock:mykey", "1", "EX", 30, "NX")
    })

    it("returns true when lock is acquired", async () => {
      const { redis, channel } = createChannelPair()
      redis.set.mockResolvedValue("OK")

      expect(await channel.acquire("k")).toBe(true)
    })

    it("returns false when lock is held", async () => {
      const { redis, channel } = createChannelPair()
      redis.set.mockResolvedValue(null)

      expect(await channel.acquire("k")).toBe(false)
    })

    it("uses custom lockTTL", async () => {
      const { redis, channel } = createChannelPair({ lockTTL: 60 })
      redis.set.mockResolvedValue("OK")

      await channel.acquire("k")

      expect(redis.set).toHaveBeenCalledWith("sf:lock:k", "1", "EX", 60, "NX")
    })
  })

  describe("release", () => {
    it("publishes and deletes lock atomically via MULTI/EXEC", async () => {
      const { redis, channel } = createChannelPair()
      const message = JSON.stringify({ ok: true, value: { data: 42 } })

      await channel.release("k", { ok: true, value: { data: 42 } })

      expect(redis.multi).toHaveBeenCalled()
      const chain = redis.getLastMulti()
      expect(chain?.publish).toHaveBeenCalledWith("sf:ch:k", message)
      expect(chain?.del).toHaveBeenCalledWith("sf:lock:k")
      expect(chain?.exec).toHaveBeenCalled()
    })

    it("serializes errors with message, name, and stack", async () => {
      const { redis, channel } = createChannelPair()
      const error = new TypeError("bad input")

      await channel.release("k", { ok: false, error })

      const chain = redis.getLastMulti()
      const published = JSON.parse(chain?.publish.mock.calls[0][1] ?? "{}")
      expect(published.ok).toBe(false)
      expect(published.error.message).toBe("bad input")
      expect(published.error.name).toBe("TypeError")
      expect(published.error.stack).toBeDefined()
    })

    it("serializes non-Error values", async () => {
      const { redis, channel } = createChannelPair()

      await channel.release("k", { ok: false, error: "string error" })

      const chain = redis.getLastMulti()
      const published = JSON.parse(chain?.publish.mock.calls[0][1] ?? "{}")
      expect(published.error.message).toBe("string error")
      expect(published.error.name).toBe("Error")
    })
  })

  describe("message routing", () => {
    it("resolves subscriber when message arrives", async () => {
      const { subscriber, channel } = createChannelPair()

      const sub = await channel.subscribe("k")

      subscriber.emit(
        "message",
        "sf:ch:k",
        JSON.stringify({ ok: true, value: "hello" }),
      )

      expect(await sub.result).toEqual({ ok: true, value: "hello" })
    })

    it("deserializes error results", async () => {
      const { subscriber, channel } = createChannelPair()

      const sub = await channel.subscribe("k")

      subscriber.emit(
        "message",
        "sf:ch:k",
        JSON.stringify({
          ok: false,
          error: { message: "fail", name: "RangeError", stack: "stack..." },
        }),
      )

      const result = await sub.result
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error).toBeInstanceOf(Error)
        expect(result.error.message).toBe("fail")
        expect(result.error.name).toBe("RangeError")
        expect(result.error.stack).toBe("stack...")
      }
    })

    it("resolves multiple subscribers for the same key", async () => {
      const { subscriber, channel } = createChannelPair()

      const sub1 = await channel.subscribe("k")
      const sub2 = await channel.subscribe("k")

      subscriber.emit(
        "message",
        "sf:ch:k",
        JSON.stringify({ ok: true, value: "shared" }),
      )

      expect(await sub1.result).toEqual({ ok: true, value: "shared" })
      expect(await sub2.result).toEqual({ ok: true, value: "shared" })
    })

    it("cleans up listeners and unsubscribes after message", async () => {
      const { subscriber, channel } = createChannelPair()

      await channel.subscribe("k")

      subscriber.emit(
        "message",
        "sf:ch:k",
        JSON.stringify({ ok: true, value: 1 }),
      )

      await new Promise((resolve) => setTimeout(resolve, 0))

      expect(subscriber.unsubscribe).toHaveBeenCalledWith("sf:ch:k")
    })

    it("delivers error result on malformed message instead of crashing", async () => {
      const { subscriber, channel } = createChannelPair()

      const sub = await channel.subscribe("k")

      subscriber.emit("message", "sf:ch:k", "NOT VALID JSON {{{")

      const result = await sub.result
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error).toBeInstanceOf(Error)
        expect(result.error.message).toContain("deserialize")
      }
    })

    it("ignores messages for unknown channels", async () => {
      const { subscriber, channel } = createChannelPair()

      await channel.subscribe("k")

      subscriber.emit(
        "message",
        "sf:ch:other",
        JSON.stringify({ ok: true, value: "x" }),
      )
    })
  })

  describe("dispose", () => {
    it("disconnects owned subscriber", async () => {
      const redis = createMockRedis()
      const duplicated = createMockRedis()
      redis.duplicate.mockReturnValue(duplicated)

      const channel = new RedisChannel(redis as never)

      await channel.dispose()

      expect(duplicated.disconnect).toHaveBeenCalled()
    })

    it("does not disconnect externally-provided subscriber", async () => {
      const { subscriber, channel } = createChannelPair()

      await channel.dispose()

      expect(subscriber.disconnect).not.toHaveBeenCalled()
    })
  })

  describe("end-to-end with SingleFlight", () => {
    it("executor path: subscribe -> acquire(true) -> execute -> release", async () => {
      const { redis, subscriber, channel } = createChannelPair()
      redis.set.mockResolvedValue("OK")

      const sf = new SingleFlightGroup({ channel })
      const result = await sf.execute("k", async () => "computed")

      const message = JSON.stringify({ ok: true, value: "computed" })

      expect(result).toBe("computed")
      expect(subscriber.subscribe).toHaveBeenCalledWith("sf:ch:k")
      expect(redis.set).toHaveBeenCalledWith("sf:lock:k", "1", "EX", 30, "NX")
      expect(redis.multi).toHaveBeenCalled()
      const chain = redis.getLastMulti()
      expect(chain?.publish).toHaveBeenCalledWith("sf:ch:k", message)
      expect(chain?.del).toHaveBeenCalledWith("sf:lock:k")
    })

    it("waiter path: subscribe -> acquire(false) -> wait for result", async () => {
      const { redis, subscriber, channel } = createChannelPair()
      redis.set.mockResolvedValue(null)

      const sf = new SingleFlightGroup({ channel })
      const promise = sf.execute("k", async () => "should-not-run")

      await new Promise((resolve) => setTimeout(resolve, 0))
      subscriber.emit(
        "message",
        "sf:ch:k",
        JSON.stringify({ ok: true, value: "from-other-process" }),
      )

      expect(await promise).toBe("from-other-process")
    })

    it("waiter receives error from executor on another process", async () => {
      const { redis, subscriber, channel } = createChannelPair()
      redis.set.mockResolvedValue(null)

      const sf = new SingleFlightGroup({ channel })
      const promise = sf.execute("k", async () => "unused")

      await new Promise((resolve) => setTimeout(resolve, 0))
      subscriber.emit(
        "message",
        "sf:ch:k",
        JSON.stringify({
          ok: false,
          error: { message: "remote failure", name: "Error" },
        }),
      )

      await expect(promise).rejects.toThrow("remote failure")
    })
  })
})
