import {
  SingleFlightGroup,
  withSingleFlight,
  SingleFlight,
  SingleFlightTimeoutError,
} from "../src"
import type { Channel, Result, Subscription } from "../src"
import { LocalChannel } from "../src/channels/local"

function deferred<T = void>() {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

describe("SingleFlight (no channel)", () => {
  it("executes the function and returns the result", async () => {
    const sf = new SingleFlightGroup()
    const result = await sf.execute("key", async () => 42)
    expect(result).toBe(42)
  })

  it("deduplicates concurrent calls with the same key", async () => {
    const sf = new SingleFlightGroup()
    let execCount = 0
    const gate = deferred()

    const fn = async () => {
      execCount++
      await gate.promise
      return "result"
    }

    const p1 = sf.execute("key", fn)
    const p2 = sf.execute("key", fn)
    const p3 = sf.execute("key", fn)

    expect(execCount).toBe(1)
    gate.resolve()

    const [r1, r2, r3] = await Promise.all([p1, p2, p3])
    expect(r1).toBe("result")
    expect(r2).toBe("result")
    expect(r3).toBe("result")
    expect(execCount).toBe(1)
  })

  it("executes independently for different keys", async () => {
    const sf = new SingleFlightGroup()
    let execCount = 0

    const fn = async () => {
      execCount++
      return execCount
    }

    const [r1, r2] = await Promise.all([
      sf.execute("a", fn),
      sf.execute("b", fn),
    ])

    expect(execCount).toBe(2)
    expect(r1).not.toBe(r2)
  })

  it("propagates errors to all waiters", async () => {
    const sf = new SingleFlightGroup()
    const gate = deferred()

    const fn = async () => {
      await gate.promise
      throw new Error("boom")
    }

    const p1 = sf.execute("key", fn)
    const p2 = sf.execute("key", fn)

    gate.resolve()

    await expect(p1).rejects.toThrow("boom")
    await expect(p2).rejects.toThrow("boom")
  })

  it("allows new execution after previous completes", async () => {
    const sf = new SingleFlightGroup()
    let execCount = 0

    const r1 = await sf.execute("key", async () => ++execCount)
    const r2 = await sf.execute("key", async () => ++execCount)

    expect(r1).toBe(1)
    expect(r2).toBe(2)
    expect(execCount).toBe(2)
  })

  it("allows new execution after previous errors", async () => {
    const sf = new SingleFlightGroup()
    let calls = 0

    await expect(
      sf.execute("key", async () => {
        calls++
        throw new Error("fail")
      }),
    ).rejects.toThrow("fail")

    const result = await sf.execute("key", async () => {
      calls++
      return "ok"
    })

    expect(result).toBe("ok")
    expect(calls).toBe(2)
  })

  it("forget() allows re-execution of an in-flight key", async () => {
    const sf = new SingleFlightGroup()
    let execCount = 0
    const gate1 = deferred()
    const gate2 = deferred()

    const p1 = sf.execute("key", async () => {
      execCount++
      await gate1.promise
      return "first"
    })

    sf.forget("key")

    const p2 = sf.execute("key", async () => {
      execCount++
      await gate2.promise
      return "second"
    })

    gate1.resolve()
    gate2.resolve()

    expect(await p1).toBe("first")
    expect(await p2).toBe("second")
    expect(execCount).toBe(2)
  })
})

describe("SingleFlight with LocalChannel", () => {
  it("deduplicates across SingleFlight instances sharing a channel", async () => {
    const channel = new LocalChannel()
    const sf1 = new SingleFlightGroup({ channel })
    const sf2 = new SingleFlightGroup({ channel })

    let execCount = 0
    const gate = deferred()

    const fn = async () => {
      execCount++
      await gate.promise
      return "shared-result"
    }

    const p1 = sf1.execute("key", fn)
    await new Promise((resolve) => setTimeout(resolve, 0))
    const p2 = sf2.execute("key", fn)

    gate.resolve()

    const [r1, r2] = await Promise.all([p1, p2])
    expect(r1).toBe("shared-result")
    expect(r2).toBe("shared-result")
    expect(execCount).toBe(1)
  })

  it("deduplicates when both instances subscribe in the same tick", async () => {
    const channel = new LocalChannel()
    const sf1 = new SingleFlightGroup({ channel })
    const sf2 = new SingleFlightGroup({ channel })

    let execCount = 0
    const gate = deferred()

    const fn = async () => {
      execCount++
      await gate.promise
      return "concurrent-result"
    }

    const p1 = sf1.execute("key", fn)
    const p2 = sf2.execute("key", fn)

    gate.resolve()

    const [r1, r2] = await Promise.all([p1, p2])
    expect(r1).toBe("concurrent-result")
    expect(r2).toBe("concurrent-result")
    expect(execCount).toBe(1)
  })

  it("propagates errors across instances", async () => {
    const channel = new LocalChannel()
    const sf1 = new SingleFlightGroup({ channel })
    const sf2 = new SingleFlightGroup({ channel })

    const gate = deferred()

    const fn = async () => {
      await gate.promise
      throw new Error("distributed-boom")
    }

    const p1 = sf1.execute("key", fn)
    await new Promise((resolve) => setTimeout(resolve, 0))
    const p2 = sf2.execute("key", fn)

    gate.resolve()

    await expect(p1).rejects.toThrow("distributed-boom")
    await expect(p2).rejects.toThrow("distributed-boom")
  })

  it("allows re-execution after release", async () => {
    const channel = new LocalChannel()
    const sf = new SingleFlightGroup({ channel })

    const r1 = await sf.execute("key", async () => "first")
    const r2 = await sf.execute("key", async () => "second")

    expect(r1).toBe("first")
    expect(r2).toBe("second")
  })

  it("handles multiple keys independently", async () => {
    const channel = new LocalChannel()
    const sf1 = new SingleFlightGroup({ channel })
    const sf2 = new SingleFlightGroup({ channel })

    const [r1, r2] = await Promise.all([
      sf1.execute("a", async () => "alpha"),
      sf2.execute("b", async () => "beta"),
    ])

    expect(r1).toBe("alpha")
    expect(r2).toBe("beta")
  })
})

describe("Channel interface contract", () => {
  it("custom channel implementation works with SingleFlight", async () => {
    const calls: string[] = []
    let storedResolver: ((result: Result) => void) | undefined

    const mockChannel: Channel = {
      async subscribe(key) {
        calls.push(`subscribe:${key}`)
        let resolver!: (result: Result) => void
        const result = new Promise<Result>((resolve) => {
          resolver = resolve
        })
        storedResolver = resolver
        return {
          result,
          unsubscribe() {
            calls.push(`unsubscribe:${key}`)
          },
        }
      },
      async acquire(key) {
        calls.push(`acquire:${key}`)
        return false
      },
      async release(key, result) {
        calls.push(`release:${key}:${JSON.stringify(result)}`)
      },
    }

    const sf = new SingleFlightGroup({ channel: mockChannel })
    const promise = sf.execute("job", async () => "unused")

    storedResolver?.({ ok: true, value: "remote-value" })

    await expect(promise).resolves.toBe("remote-value")
    expect(calls).toEqual(["subscribe:job", "acquire:job"])
  })
})

describe("Decorator helpers", () => {
  it("withSingleFlight deduplicates wrapped calls", async () => {
    let calls = 0
    const gate = deferred()

    const wrapped = withSingleFlight(
      async (id: number) => {
        calls++
        await gate.promise
        return `user:${id}`
      },
      (id) => `user:${id}`,
    )

    const p1 = wrapped(1)
    const p2 = wrapped(1)

    gate.resolve()

    await expect(Promise.all([p1, p2])).resolves.toEqual(["user:1", "user:1"])
    expect(calls).toBe(1)
  })

  it("SingleFlight decorator deduplicates method calls", async () => {
    const gate = deferred()
    let calls = 0

    class TestService {
      @SingleFlight((id: number) => `user:${id}`)
      async load(id: number) {
        calls++
        await gate.promise
        return `value:${id}`
      }
    }

    const service = new TestService()
    const p1 = service.load(1)
    const p2 = service.load(1)

    gate.resolve()

    await expect(Promise.all([p1, p2])).resolves.toEqual(["value:1", "value:1"])
    expect(calls).toBe(1)
  })
})

describe("Timeout behavior", () => {
  it("rejects waiters with SingleFlightTimeoutError", async () => {
    const pending = deferred<Result>()
    const channel: Channel = {
      async subscribe() {
        return {
          result: pending.promise,
          unsubscribe() {},
        }
      },
      async acquire() {
        return false
      },
      async release() {},
    }

    const sf = new SingleFlightGroup({ channel, timeout: 10 })

    await expect(sf.execute("key", async () => "unused")).rejects.toBeInstanceOf(
      SingleFlightTimeoutError,
    )
  })
})
