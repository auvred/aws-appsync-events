import process from 'node:process'
import { setTimeout as sleep } from 'node:timers/promises'
import { vi, expect, test } from 'vitest'
import { Client } from '../src/client.js'

const API_ENDPOINT = process.env.VITE_APPSYNC_API_ENDPOINT!
const API_KEY = process.env.VITE_APPSYNC_API_KEY!

function newClient() {
  return new Client(API_ENDPOINT, {
    type: 'API_KEY',
    key: API_KEY,
  })
}

test('publish', async () => {
  const client = newClient()
  await client.publish('default/foo', ['message', 'another message'])
})

test('publish error missing events', async () => {
  const client = newClient()

  await expect(async () => {
    await client.publish('default/foo', [])
  }).rejects.toThrow(/Missing events/)
})

test('subscribe error', async () => {
  const next = vi.fn()
  const error = vi.fn()
  const client = newClient()
  const sub = client.subscribe('non-existent-channel', { next, error })

  await expect(sub.promise).rejects.toThrow(/UnauthorizedException/)

  expect(next).not.toBeCalled()
  expect(error).toHaveBeenCalledExactlyOnceWith(new Error('Subscribe error: UnauthorizedException'))
})

test('multiple listeners', async () => {
  const client = newClient()

  const { promise, resolve } = Promise.withResolvers<void>()
  let counter = 0
  const next = vi.fn(() => ++counter == 2 && resolve())
  
  const sub1 = client.subscribe('default/foo', { next })
  const sub2 = client.subscribe('default/foo', { next })

  await Promise.all([sub1.promise, sub2.promise])

  await client.publish('default/foo', [{ data: [123, 'foo'] }])

  await promise

  expect(next).toHaveBeenNthCalledWith(2, { data: [123, 'foo'] })
})

test('unsubscribe', async () => {
  const client = newClient()
const next1 = vi.fn()
  const error1 = vi.fn()
  const next2 = vi.fn()
  const error2 = vi.fn()

  const sub1 = client.subscribe('default/foo', { next: next1, error: error1 })
  const sub2 = client.subscribe('default/*', { next: next2, error: error2 })
  sub1.unsubscribe()
  sub1.unsubscribe()
  sub1.unsubscribe()

  await Promise.all([
    sub1.promise, 
    sub2.promise,
  ])
  await client.publish('default/foo', ['msg'])

  await vi.waitUntil(() => next2.mock.calls.length > 0)

  expect(next1).not.toBeCalled()
  expect(error1).not.toBeCalled()
  expect(next2).toHaveBeenCalledExactlyOnceWith('msg')
  expect(error2).not.toBeCalled()
})

// flaky
test.skip('unsubscribe while handshaking', async () => {
  const client = newClient()

  const next1 = vi.fn()
  const error1 = vi.fn()
  const next2 = vi.fn(console.log)
  const error2 = vi.fn()

  const sub1 = client.subscribe('default/foo', { next: next1, error: error1 })
  const sub2 = client.subscribe('default/*', { next: next2, error: error2 })

  await vi.waitUntil(() => client['state'].type === 'handshaking')

  sub1.unsubscribe()

  await Promise.all([
    sub1.promise, 
    sub2.promise,
  ])

  await client.publish('default/foo', ['msg'])

  await vi.waitUntil(() => next2.mock.calls.length > 0)

  expect(next1).not.toBeCalled()
  expect(error1).not.toBeCalled()
  expect(next2).toHaveBeenCalledExactlyOnceWith('msg')
  expect(error2).not.toBeCalled()
})

test('two similar listeners', async () => {
  const client = newClient()

  const next1 = vi.fn()
  const error1 = vi.fn()
  const next2 = vi.fn()
  const error2 = vi.fn()

  const sub1 = client.subscribe('default/foo', { next: next1, error: error1 })
  const sub2 = client.subscribe('default/foo', { next: next2, error: error2 })

  await Promise.all([
    sub1.promise, 
    sub2.promise,
  ])
  await client.publish('default/foo', ['msg'])

  await vi.waitUntil(() => next1.mock.calls.length > 0 && next2.mock.calls.length > 0)

  expect(next1).toHaveBeenCalledExactlyOnceWith('msg')
  expect(error1).not.toBeCalled()
  expect(next2).toHaveBeenCalledExactlyOnceWith('msg')
  expect(error2).not.toBeCalled()
})

test('two similar listeners, unsubscribe', async () => {
  const client = newClient()

  const next1 = vi.fn()
  const error1 = vi.fn()
  const next2 = vi.fn()
  const error2 = vi.fn()

  const sub1 = client.subscribe('default/foo', { next: next1, error: error1 })
  const sub2 = client.subscribe('default/foo', { next: next2, error: error2 })
  sub1.unsubscribe()

  await Promise.all([
    sub1.promise, 
    sub2.promise,
  ])
  await client.publish('default/foo', ['msg'])

  await vi.waitUntil(() => next2.mock.calls.length > 0)

  expect(next1).not.toBeCalled()
  expect(error1).not.toBeCalled()
  expect(next2).toHaveBeenCalledExactlyOnceWith('msg')
  expect(error2).not.toBeCalled()
})
