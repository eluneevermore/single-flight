import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    globals: true,
    testTimeout: 60e3,
    pool: "forks",
    maxWorkers: 16,
    maxConcurrency: 64,
  },
})
