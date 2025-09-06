import process from 'node:process'
import { vi, expect, test, assert } from 'vitest'
import { apiKeyAuthorizer, awsIamAuthorizer, Client } from '../src/client.js'
import { expectUncaughtException } from './utils.js'

const API_ENDPOINT = process.env.VITE_APPSYNC_API_ENDPOINT!
const API_KEY = process.env.VITE_APPSYNC_API_KEY!
const ACCESS_KEY_ID = process.env.VITE_APPSYNC_ACCESS_KEY_ID!
const SECRET_ACCESS_KEY = process.env.VITE_APPSYNC_SECRET_ACCESS_KEY!
const REGION = process.env.VITE_APPSYNC_REGION!

test('api key auth', async () => {
  const client = new Client(API_ENDPOINT, apiKeyAuthorizer(API_KEY))

  const event = vi.fn()
  const established = vi.fn()
  client.subscribe('default/foo', { event, established })

  await expect.poll(() => established).toHaveBeenCalledOnce()

  await client.publish('default/foo', ['foo', { bar: [123] }])

  await expect.poll(() => event).toHaveBeenCalledTimes(2)

  assert.sameDeepMembers(event.mock.calls, [['foo'], [{ bar: [123] }]])
})

test('invalid api key', async () => {
  expectUncaughtException('[aws-appsync-events] connection error: UnauthorizedException 401 (You are not authorized to make this call.)')

  const onStateChanged = vi.fn()
  const client = new Client(API_ENDPOINT, apiKeyAuthorizer(API_KEY + 'invalid'), {
    onStateChanged,
  })

  const established = vi.fn()
  const error = vi.fn()
  client.subscribe('default/foo', { event: () => {}, established, error })

  await expect.poll(() => onStateChanged).toHaveBeenCalledWith('failed')
})

test('websocket retry', async () => {
  const retry = vi.fn((attempt: number) => attempt < 3 && 10)
  const onStateChanged = vi.fn()
  const client = new Client('nonexistent123123123121231.appsync-api.eu-central-1.amazonaws.com', apiKeyAuthorizer(API_KEY), { retryBehavior: retry, onStateChanged })

  client.subscribe('default/foo', { event: () => {} })

  await expect.poll(() => onStateChanged).toHaveBeenCalledWith('failed')
  expect(onStateChanged.mock.calls).toEqual([['connecting'], ['backoff'], ['connecting'], ['backoff'], ['connecting'], ['failed']])
  expect(retry).toHaveBeenCalledTimes(3)
})

test('publish batch split', async () => {
  const client = new Client(API_ENDPOINT, apiKeyAuthorizer(API_KEY))

  const event = vi.fn()
  const established = vi.fn()
  client.subscribe('default/foo', { event, established })

  await expect.poll(() => established).toHaveBeenCalledOnce()

  const events = new Array(7).fill(null).map((_, i) => i)
  await client.publish('default/foo', events)

  await expect.poll(() => event).toHaveBeenCalledTimes(events.length)

  assert.sameDeepMembers(event.mock.calls, events.map(e => [e]))
})

test('publish error', async () => {
  const retry = vi.fn((attempt: number) => attempt < 2 && 10)
  const client = new Client('nonexistent123123123121231.appsync-api.eu-central-1.amazonaws.com', apiKeyAuthorizer(API_KEY), { retryBehavior: retry })

  await expect(async () => {
    await client.publish('default/foo', [123])
  }).rejects.toThrow()
  expect(retry).toHaveBeenCalledTimes(2)
})

test('custom authorizer', async () => {
  const authorizer = awsIamAuthorizer({
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
    region: REGION,
  })
  const client = new Client(API_ENDPOINT, apiKeyAuthorizer(API_KEY))

  const event = vi.fn()
  const established = vi.fn()
  client.subscribe('iam-auth/foo', { event, established, authorizer })

  await expect.poll(() => established).toHaveBeenCalledOnce()

  await client.publish('iam-auth/foo', ['foo', { bar: [123] }], { authorizer })

  await expect.poll(() => event).toHaveBeenCalledTimes(2)

  assert.sameDeepMembers(event.mock.calls, [['foo'], [{ bar: [123] }]])
})

test('aws iam auth', async () => {
  const client = new Client(API_ENDPOINT, awsIamAuthorizer({
    accessKeyId: ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY,
    region: REGION,
  }))

  const event = vi.fn()
  const established = vi.fn()
  client.subscribe('iam-auth/foo', { event, established })

  await expect.poll(() => established).toHaveBeenCalledOnce()

  await client.publish('iam-auth/foo', ['foo', { bar: [123] }])

  await expect.poll(() => event).toHaveBeenCalledTimes(2)

  assert.sameDeepMembers(event.mock.calls, [['foo'], [{ bar: [123] }]])
})
