import { expect, onTestFinished, vi } from "vitest"
import { setTimeout as sleep } from 'node:timers/promises'

export function expectMockCalledWithError(mock: ReturnType<typeof vi.fn>, msg: string) {
  expect(mock).toHaveBeenCalledOnce()
  const err = mock.mock.lastCall![0] 
  expect(err).toBeInstanceOf(Error)
  expect((err as Error).message).toMatch(msg)
}
export function expectUncaughtException(msg: string) {
  const onUncaughtException = vi.fn()
  process.once('uncaughtException', onUncaughtException)
  onTestFinished(async () => {
    await sleep(0)
    process.removeListener('uncaughtException', onUncaughtException)

    expectMockCalledWithError(onUncaughtException, msg)
  })
}
