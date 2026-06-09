# single-flight

Deduplicate concurrent function calls. When multiple callers request the same work simultaneously, only one execution runs and all callers share the result.

Works across processes and machines via pluggable **Channel** implementations (Redis, IPC, Kafka, etc.).

## Installation

```bash
pnpm add single-flight
```

If you want the Redis channel:

```bash
pnpm add single-flight ioredis
```

## Usage

### Direct

```ts
import { SingleFlightGroup } from "single-flight"

const sf = new SingleFlightGroup()

// Both calls share the same execution
const [a, b] = await Promise.all([
  sf.execute("user:123", () => fetchUser(123)),
  sf.execute("user:123", () => fetchUser(123)),
])
// fetchUser called once, a === b
```

### Wrapper function

```ts
import { withSingleFlight } from "single-flight"

const getUser = withSingleFlight(
  (id: number) => fetchUser(id),
  (id) => `user:${id}`,
)

// Concurrent calls with the same key share one execution
await Promise.all([getUser(1), getUser(1), getUser(2)])
```

### Decorator

```ts
import { SingleFlight } from "single-flight"

class UserService {
  @SingleFlight((id: number) => `user:${id}`)
  async getUser(id: number) {
    return await db.query("SELECT * FROM users WHERE id = ?", [id])
  }
}
```

## Distributed usage

Without a channel, deduplication is local to the process. To deduplicate across processes or machines, provide a `Channel` implementation:

```ts
import { SingleFlightGroup } from "single-flight"
import { RedisChannel } from "single-flight/channels/redis"

const sf = new SingleFlightGroup({
  channel: new RedisChannel(redisClient),
  timeout: 30_000,
})
```

### Timeout

When using a channel, set `timeout` (in milliseconds) to prevent waiters from hanging forever if the executor crashes:

```ts
const sf = new SingleFlightGroup({
  channel: new RedisChannel(redis),
  timeout: 30_000,
})
```

Without a timeout, if the executor process crashes after acquiring the lock but before publishing the result, all waiters on other processes will hang indefinitely.

The timeout only affects the **waiter path** (callers that do not acquire the lock). It does not limit the executor. Use your own timeout on `fn()` for that.

On timeout, waiters reject with `SingleFlightTimeoutError` and automatically clean up their channel subscription.

## Channel interface

A channel coordinates lock acquisition and result broadcasting across processes. The protocol is **subscribe -> acquire -> release**:

```ts
import type { Channel, Result, Subscription } from "single-flight"

class MyChannel implements Channel {
  async subscribe(key: string): Promise<Subscription> { /* ... */ }
  async acquire(key: string): Promise<boolean> { /* ... */ }
  async release(key: string, result: Result): Promise<void> { /* ... */ }
  async dispose(): Promise<void> { /* ... */ }
}
```

`subscribe` must be called before `acquire` to avoid a race where the result is published between the lock check and subscription setup. `SingleFlightGroup` handles this ordering automatically.

### Built-in channels

| Channel | Import | Scope |
|---------|--------|-------|
| `LocalChannel` | `single-flight/channels/local` | Single process (testing) |
| `RedisChannel` | `single-flight/channels/redis` | Distributed (multi-process/machine) |

## API

### `SingleFlightGroup`

| Method | Description |
|--------|-------------|
| `execute<T>(key, fn)` | Run `fn` under deduplication for `key` |
| `forget(key)` | Remove key from in-flight map, allowing re-execution |
| `dispose()` | Cleanup the channel |

Options:

| Option | Type | Description |
|--------|------|-------------|
| `channel` | `Channel` | Channel for distributed coordination |
| `timeout` | `number` | Waiter timeout in ms. Rejects with `SingleFlightTimeoutError` on expiry |

### `@SingleFlight(keyFn, options?)`

Method decorator that deduplicates calls based on the key returned by `keyFn`.

### `withSingleFlight(fn, keyFn, options?)`

Wraps a function with deduplication. Returns a new function with the same signature.

## Release

This package is published from GitHub Actions.

1. Update `package.json` with the next semver version.
2. Commit the change and push it.
3. Create and push a matching tag such as `v0.1.3`.
4. The publish workflow verifies the tag matches `package.json`, then runs `npm publish --provenance`.
