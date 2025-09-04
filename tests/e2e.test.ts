import process from 'node:process'
import { vi, expect, test, assert } from 'vitest'
import { apiKeyAuthorizer, awsIamAuthorizer, Client } from '../src/client.js'

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
