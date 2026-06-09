import type { Channel, Result, Subscription } from "../types"

interface LockEntry {
  acquired: boolean
  subscribers: Array<(result: Result) => void>
}

/**
 * In-process channel for single-process use and testing.
 *
 * Simulates the distributed Channel protocol entirely in memory.
 * Useful for unit tests and applications that only need
 * cross-instance (not cross-process) deduplication.
 */
export class LocalChannel implements Channel {
  private readonly entries = new Map<string, LockEntry>()

  async subscribe(key: string): Promise<Subscription> {
    let resolver!: (result: Result) => void

    const result = new Promise<Result>((resolve) => {
      resolver = resolve
    })

    let entry = this.entries.get(key)
    if (!entry) {
      entry = { acquired: false, subscribers: [] }
      this.entries.set(key, entry)
    }
    entry.subscribers.push(resolver)

    return {
      result,
      unsubscribe: () => {
        const currentEntry = this.entries.get(key)
        if (!currentEntry) return
        const subscriberIndex = currentEntry.subscribers.indexOf(resolver)
        if (subscriberIndex !== -1) {
          currentEntry.subscribers.splice(subscriberIndex, 1)
        }
      },
    }
  }

  async acquire(key: string): Promise<boolean> {
    const entry = this.entries.get(key)
    if (entry?.acquired) return false

    if (entry) {
      entry.acquired = true
      return true
    }

    this.entries.set(key, { acquired: true, subscribers: [] })
    return true
  }

  async release(key: string, result: Result): Promise<void> {
    const entry = this.entries.get(key)
    if (!entry) return

    this.entries.delete(key)
    for (const resolve of entry.subscribers) {
      resolve(result)
    }
  }
}
