import { expect, describe, test, vi, afterEach, beforeEach, onTestFinished } from 'vitest'
import { setTimeout as sleep } from 'node:timers/promises'
import { Client, exponentialBackoffRetryBehavior, parseEndpoint, ResettableTimer, type ClientOpts, type WebSocketAdapter, type WebSocketAdapterConstructor } from '../src/client.js'
import * as util from 'node:util'

function expectMockCalledWithError(mock: ReturnType<typeof vi.fn>, msg: string) {
  expect(mock).toHaveBeenCalledOnce()
  const err = mock.mock.lastCall![0] 
  expect(err).toBeInstanceOf(Error)
  expect((err as Error).message).toMatch(msg)
}
function expectUncaughtException(msg: string) {
  const onUncaughtException = vi.fn()
  process.once('uncaughtException', onUncaughtException)
  onTestFinished(async () => {
    await sleep(0)
    process.removeListener('uncaughtException', onUncaughtException)

    expectMockCalledWithError(onUncaughtException, msg)
  })
}

describe('parseEndpoint', () => {
  describe('provided', () => {
    test('raw http endpoint', () => {
      const { http, realtime } = parseEndpoint('example123456789example123.appsync-api.us-east-1.amazonaws.com')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('other regions', () => {
      const { http, realtime } = parseEndpoint('example123456789example123.appsync-api.eu-central-1.amazonaws.com')

      expect(http).toBe('example123456789example123.appsync-api.eu-central-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.eu-central-1.amazonaws.com')
    })

    test('amazonaws.com.cn', () => {
      const { http, realtime } = parseEndpoint('example123456789example123.appsync-api.us-east-1.amazonaws.com.cn')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com.cn')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com.cn')
    })

    test('raw realtime endpoint', () => {
      const { http, realtime } = parseEndpoint('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('wss:// realtime endpoint', () => {
      const { http, realtime } = parseEndpoint('wss://example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('ws:// realtime endpoint', () => {
      const { http, realtime } = parseEndpoint('wss://example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('https:// realtime endpoint', () => {
      const { http, realtime } = parseEndpoint('https://example123456789example123.appsync-api.us-east-1.amazonaws.com')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('http:// realtime endpoint', () => {
      const { http, realtime } = parseEndpoint('http://example123456789example123.appsync-api.us-east-1.amazonaws.com')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('trailing slash', () => {
      const { http, realtime } = parseEndpoint('example123456789example123.appsync-api.us-east-1.amazonaws.com/')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('/event', () => {
      const { http, realtime } = parseEndpoint('example123456789example123.appsync-api.us-east-1.amazonaws.com/event')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('/event/', () => {
      const { http, realtime } = parseEndpoint('example123456789example123.appsync-api.us-east-1.amazonaws.com/event/')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('/event/realtime', () => {
      const { http, realtime } = parseEndpoint('example123456789example123.appsync-api.us-east-1.amazonaws.com/event/realtime')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })

    test('/event/realtime/', () => {
      const { http, realtime } = parseEndpoint('example123456789example123.appsync-api.us-east-1.amazonaws.com/event/realtime/')

      expect(http).toBe('example123456789example123.appsync-api.us-east-1.amazonaws.com')
      expect(realtime).toBe('example123456789example123.appsync-realtime-api.us-east-1.amazonaws.com')
    })
  })

  describe('custom', () => {
    test('raw domain', () => {
      const { http, realtime } = parseEndpoint('example.com')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('wss://', () => {
      const { http, realtime } = parseEndpoint('wss://example.com')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('ws://', () => {
      const { http, realtime } = parseEndpoint('ws://example.com')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('https://', () => {
      const { http, realtime } = parseEndpoint('https://example.com')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('http://', () => {
      const { http, realtime } = parseEndpoint('http://example.com')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('trailing slash', () => {
      const { http, realtime } = parseEndpoint('example.com/')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('/event', () => {
      const { http, realtime } = parseEndpoint('example.com/event')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('/event/', () => {
      const { http, realtime } = parseEndpoint('example.com/event/')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('/event/realtime', () => {
      const { http, realtime } = parseEndpoint('example.com/event/realtime')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('/event/realtime/', () => {
      const { http, realtime } = parseEndpoint('example.com/event/realtime/')

      expect(http).toBe('example.com')
      expect(realtime).toBe('example.com')
    })

    test('unicode', () => {
      const { http, realtime } = parseEndpoint('exámple.com')

      expect(http).toBe('xn--exmple-qta.com')
      expect(realtime).toBe('xn--exmple-qta.com')
    })

    test('invalid punycode', () => {
      expect(() => parseEndpoint('xn--example.com')).toThrow(TypeError)
    })
  })
})

describe('ResettableTimer', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })
  afterEach(() => {
    vi.restoreAllMocks()
  })

  test('timeouts', () => {
    const timeout = vi.fn()
    new ResettableTimer(50, timeout)

    vi.advanceTimersByTime(75)
    
    expect(timeout).toHaveBeenCalledOnce()
  })

  test('can be resetted', () => {
    const timeout = vi.fn()
    const timer = new ResettableTimer(50, timeout)

    vi.advanceTimersByTime(25)
    timer.reset()
    vi.advanceTimersByTime(25)
    timer.reset()
    vi.advanceTimersByTime(25)
    
    expect(timeout).not.toBeCalled()
  })

  test('works after timeout+reset', () => {
    const timeout = vi.fn()
    const timer = new ResettableTimer(50, timeout)

    vi.advanceTimersByTime(75)

    expect(timeout).toHaveBeenCalledOnce()
    timeout.mockClear()
    timer.reset()
    vi.advanceTimersByTime(75)

    expect(timeout).toHaveBeenCalledOnce()
  })

  test('can be cancelled', () => {
    const timeout = vi.fn()
    const timer = new ResettableTimer(50, timeout)

    vi.advanceTimersByTime(25)
    timer.cancel()
    vi.advanceTimersByTime(75)
    
    expect(timeout).not.toBeCalled()
  })

  test('cancel after timeout does nothing', () => {
    const timeout = vi.fn()
    const timer = new ResettableTimer(50, timeout)

    vi.advanceTimersByTime(75)
    timer.cancel()
    vi.advanceTimersByTime(75)
    
    expect(timeout).toHaveBeenCalledOnce()
  })
})

describe('Client', { timeout: 100 }, () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })
  afterEach(() => {
    vi.runAllTimers()
    vi.clearAllTimers()
    vi.restoreAllMocks()
  })

  type SocketAdapter = Pick<EventTarget, 'dispatchEvent'> & Pick<WebSocketAdapter, 'send' | 'close'> & {
    readonly incomingMessages: string[]
  }

  function subscribeWithMocks(client: Client, channel: string) {
    const event = vi.fn()
    const established = vi.fn()
    const error = vi.fn()

    return {
      sub: client.subscribe(channel, { event, established, error }),
      event,
      established,
      error
    }
  }

  class Socket {
    constructor(readonly adapter: SocketAdapter) {}

    open = () => {
      this.adapter.dispatchEvent(new Event('open'))
    }

    close = () => {
      this.adapter.dispatchEvent(new CloseEvent('close', { code: 1006 }))
    }

    error = () => {
      this.adapter.dispatchEvent(new Event('error'))
    }

    send = (msg: unknown) => {
      this.adapter.dispatchEvent(new MessageEvent('message', {
        data: JSON.stringify(msg),
      }))
    }

    sendData = (subId: string, data: unknown) => {
      this.send({ type: 'data', id: subId, event: JSON.stringify(data) })
    }

    consumeMessage = () => {
      const message = this.adapter.incomingMessages.shift()
      if (message == null) {
        return undefined
      }
      return JSON.parse(message)
    }

    consumeSubscribeRequest = (channel: string ) => {
      const msg = this.consumeMessage()
      expect(msg).toEqual({
        type: 'subscribe',
        id: expect.any(String),
        channel,
        authorization: expect.any(Object),
      })
      return msg.id
    }

    subscribeSuccess = (id: string) => {
      this.send({ type: 'subscribe_success', id })
    }

    acceptSubscribe = (channel: string) => {
      const id = this.consumeSubscribeRequest(channel)
      this.subscribeSuccess(id)
      return id
    }

    consumeUnsubscribeRequest = (id: string) => {
      expect(this.consumeMessage()).toEqual({
        type: 'unsubscribe',
        id,
      })
    }

    acceptUnsubscribe = (id: string) => {
      this.consumeUnsubscribeRequest(id)
      this.send({
        type: 'unsubscribe_success',
        id,
      })
    }

    openAndHandshake = (connectionTimeoutMs?: number | undefined) => {
      this.open()
      this.handshake(connectionTimeoutMs)
    }

    handshake = (connectionTimeoutMs: number | undefined = 30_000 ) => {
      expect(this.consumeMessage()).toEqual({ type: 'connection_init' })
      this.send({ type: 'connection_ack', connectionTimeoutMs })
    }
  }

  function newClient(opts?: ClientOpts) {
    const client = new Client('example.com', { type: 'API_KEY', key: 'foo' }, opts)
    const sockets: Socket[] = []

    // @ts-expect-error - incompatible addEventListener/removeEventListener
    class Adapter extends EventTarget implements WebSocketAdapter {
      readonly incomingMessages: string[] = []
      closed = false

      constructor(readonly url: string, readonly protocols: string[]) {
        super()
        sockets.push(new Socket(this))
      }

      send = (data: string): void => {
        this.incomingMessages.push(data)
      }
      close = (): void => {
        // TODO: explicitly test close in tests
        this.closed = true
      }
    }

    onTestFinished(() => {
      for (const [i, socket] of sockets.entries()) {
        expect(socket.adapter.incomingMessages, `sockets[${i}]: ${util.inspect(socket.adapter.incomingMessages)}`).toHaveLength(0)
      }
    })

    client['wsCtor'] = Adapter as WebSocketAdapterConstructor
    return {
      client,
      sockets: {
        get: (idx: number) => {
          expect(sockets).toHaveLength(idx + 1)
          return sockets[idx]!
        }
      }
    }
  }

  test('basic subscribe', () => {
    const { client, sockets } = newClient()

    const {
      event, 
      established,
      error,
    } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.consumeSubscribeRequest('default/foo')

    expect(established).not.toBeCalled()
    expect(error).not.toBeCalled()

    socket.send({ type: 'subscribe_success', id: subId })

    expect(established).toHaveBeenCalledOnce()
    expect(error).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })

    expect(event).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
  })

  test('subscribe - connecting', () => {
    const { client, sockets } = newClient()

    const { event: event1, established: est1, error: err1 } = subscribeWithMocks(client, 'default/foo')

    expect(client['state'].type).toBe('connecting')

    const { event: event2, established: est2, error: err2 } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake()

    const subId = socket.acceptSubscribe('default/foo')

    expect(est1).toBeCalled()
    expect(err1).not.toBeCalled()
    expect(est2).toBeCalled()
    expect(err2).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })

    expect(event1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(event2).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
  })

  test('subscribe - handshaking', () => {
    const { client, sockets } = newClient()

    const { event: event1, established: est1, error: err1 } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.open()
    expect(client['state'].type).toBe('handshaking')

    const { event: event2, established: est2, error: err2 } = subscribeWithMocks(client, 'default/foo')

    socket.handshake()

    const subId = socket.acceptSubscribe('default/foo')

    expect(est1).toBeCalled()
    expect(err1).not.toBeCalled()
    expect(est2).toBeCalled()
    expect(err2).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })

    expect(event1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(event2).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
  })

  test('subscribe - connected', () => {
    const { client, sockets } = newClient()

    const { event: event1, established: est1, error: err1 } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    const subId = socket.acceptSubscribe('default/foo')

    expect(est1).toBeCalled()
    expect(err1).not.toBeCalled()

    const { event: event2, established: est2, error: err2 } = subscribeWithMocks(client, 'default/foo')

    expect(est2).toBeCalled()
    expect(err2).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })

    expect(event1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(event2).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
  })

  test('subscribe - connected - new channel', () => {
    const { client, sockets } = newClient()

    const { event: event1, established: est1, error: err1 } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    const subId1 = socket.acceptSubscribe('default/foo')

    expect(est1).toBeCalled()
    expect(err1).not.toBeCalled()

    const { event: event2, established: est2, error: err2 } = subscribeWithMocks(client, 'default/bar')

    expect(est2).not.toBeCalled()
    expect(err2).not.toBeCalled()

    const subId2 = socket.acceptSubscribe('default/bar')

    socket.sendData(subId1, { foo: 123 })

    expect(event1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(event2).not.toBeCalled()

    event1.mockClear()
    socket.sendData(subId2, { bar: 123 })

    expect(event1).not.toBeCalled()
    expect(event2).toHaveBeenCalledExactlyOnceWith({ bar: 123 })
  })

  test('subscribe - backoff', () => {
    const { client, sockets } = newClient()

    client['connect']()
    const socket1 = sockets.get(0)
    socket1.close()
    expect(client['state'].type).toBe('backoff')

    const { event, established, error } = subscribeWithMocks(client, 'default/foo')

    expect(established).not.toBeCalled()
    expect(error).not.toBeCalled()

    vi.runAllTimers()
    expect(client['state'].type).toBe('connecting')
    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    socket2.acceptSubscribe('default/foo')

    expect(established).toBeCalled()
    expect(error).not.toBeCalled()
    expect(event).not.toBeCalled()
  })

  test('subscribe - failed', () => {
    const { client, sockets } = newClient({ retryBehavior: () => -1 })

    client['connect']()
    const socket1 = sockets.get(0)
    socket1.close()
    expect(client['state'].type).toBe('failed')

    const { event } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(1)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')
    socket.sendData(subId, { foo: 123 })
    expect(event).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
  })

  test('subscribe - open out of order', () => {
    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.close()
    expect(client['state'].type).toBe('backoff')

    vi.runAllTimers()
    expect(client['state'].type).toBe('connecting')
    socket.open()
    expect(socket.consumeMessage()).toBeUndefined()
  })

  test('subscribe - double open', () => {
    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.open()
    expect(client['state'].type).toBe('connected')
  })

  test('subscribe - message before handshake', () => {
    expectUncaughtException('handshake error: expected "connection_ack" but got "subscribe_success"')

    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.open()
    expect(socket.consumeMessage()).toEqual({ type: 'connection_init' })
    expect(client['state'].type).toBe('handshaking')
    socket.subscribeSuccess('default/foo')
  })

  test('unexpected binary message', () => {
    expectUncaughtException('unexpected binary data in message')
    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.adapter.dispatchEvent(new MessageEvent('message', {
      data: new ArrayBuffer(0),
    }))
  })

  test('unknown error', () => {
    expectUncaughtException('[aws-appsync-events] unknown error: UnsupportedOperation (Operation not supported through the realtime channel)')
    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.send({
      type: 'error',
      errors: [
        {
          errorType: 'UnsupportedOperation',
          message: 'Operation not supported through the realtime channel'
        }
      ]
    })
  })

  test('unknown message', () => {
    expectUncaughtException('unknown message: {"type":"foo","bar":123}')
    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.send({
      type: 'foo',
      bar: 123,
    })

  })

  test('unknown message - without type', () => {
    expectUncaughtException('unknown message: {"bar":123}')
    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.send({
      bar: 123,
    })

  })

  test('subscribe error', () => {
    const { client, sockets } = newClient()

    const { established, error } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.consumeSubscribeRequest('default/foo')
    socket.send({
      type: 'subscribe_error',
      id: subId,
      errors: [
        {
          errorType: 'UnauthorizedException',
        },
        {
          errorType: 'BadRequestException',
          message: 'Invalid Channel Format',
        }
      ],
    })

    expect(established).not.toBeCalled()
    expectMockCalledWithError(error, 'Subscribe error: UnauthorizedException, BadRequestException (Invalid Channel Format)')
  })

  test('subscribe error called once', () => {
    const { client, sockets } = newClient()

    const { established, error } = subscribeWithMocks(client, 'default/foo')

    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    const subId = socket1.consumeSubscribeRequest('default/foo')
    socket1.send({
      type: 'subscribe_error',
      id: subId,
      errors: [ { errorType: 'UnauthorizedException' }],
    })

    expect(established).not.toBeCalled()
    expectMockCalledWithError(error, 'Subscribe error: UnauthorizedException')
    error.mockClear()

    socket1.close()
    vi.runAllTimers()

    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    expect(socket2.consumeMessage()).toBeUndefined()
  })

  test('subscribe_success unknown id', () => {
    expectUncaughtException('[aws-appsync-events bug] subscribe_success for unknown sub')
    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.subscribeSuccess('foo')
  })

  test('subscribe_error unknown id', () => {
    expectUncaughtException('[aws-appsync-events bug] subscribe_error for unknown sub')
    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.send({
      id: 'foo',
      type: 'subscribe_error',
    })
  })

  test('unsubscribe - connecting', () => {
    const { client, sockets } = newClient()

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')
    
    expect(client['state'].type).toBe('connecting')
    sub.unsubscribe()

    expect(established).not.toBeCalled()
    expect(error).not.toBeCalled()

    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(socket.consumeMessage()).toBeUndefined()
  })

  test('double unsubscribe - connecting', () => {
    const { client, sockets } = newClient()

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')

    expect(client['state'].type).toBe('connecting')
    sub.unsubscribe()
    sub.unsubscribe()

    expect(established).not.toBeCalled()
    expect(error).not.toBeCalled()

    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(socket.consumeMessage()).toBeUndefined()
  })


  test('unsubscribe - handshaking', () => {
    const { client, sockets } = newClient()

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.open()
    
    expect(client['state'].type).toBe('handshaking')
    sub.unsubscribe()

    socket.handshake()

    expect(established).not.toBeCalled()
    expect(error).not.toBeCalled()

    expect(socket.consumeMessage()).toBeUndefined()
  })

  test('unsubscribe - connecting - two subs', () => {
    const { client, sockets } = newClient()

    const { event: event1, established: est1, error: err1 } = subscribeWithMocks(client, 'default/foo')

    const { sub: sub2, event: event2, established: est2, error: err2 } = subscribeWithMocks(client, 'default/foo')
    
    expect(client['state'].type).toBe('connecting')
    sub2.unsubscribe()

    expect(est1).not.toBeCalled()
    expect(err1).not.toBeCalled()
    expect(est2).not.toBeCalled()
    expect(err2).not.toBeCalled()

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')

    expect(est1).toHaveBeenCalledOnce()
    expect(err1).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })

    expect(event1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(event2).not.toBeCalled()
  })

  test('unsubscribe - connected - not subscribed', () => {
    const { client, sockets } = newClient()

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.consumeSubscribeRequest('default/foo')

    sub.unsubscribe()

    expect(established).not.toBeCalled()
    expect(error).not.toBeCalled()
    expect(socket.consumeMessage()).toBeUndefined()

    socket.subscribeSuccess(subId)

    expect(established).toHaveBeenCalledOnce()
    expect(error).not.toBeCalled()
    socket.acceptUnsubscribe(subId)
  })

  test('unsubscribe - connected - subscribed', () => {
    const { client, sockets } = newClient()

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')

    expect(established).toBeCalled()
    expect(error).not.toBeCalled()

    sub.unsubscribe()

    socket.acceptUnsubscribe(subId)
  })

  test('unsubscribe - connected - failed', () => {
    const { client, sockets } = newClient({ retryBehavior: exponentialBackoffRetryBehavior(0) })

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')

    sub.unsubscribe()
    socket.acceptUnsubscribe(subId)
    vi.runAllTimers()
    expect(client['state'].type).toBe('failed')

    expect(established).toBeCalled()
    expect(error).not.toBeCalled()
  })

  test('sub, backoff, unsub', () => {
    const { client, sockets } = newClient()

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')

    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    socket1.acceptSubscribe('default/foo')

    expect(established).toHaveBeenCalledOnce()
    expect(error).not.toBeCalled()

    socket1.close()
    expect(client['state'].type).toBe('backoff')
    sub.unsubscribe()

    vi.runAllTimers()

    expect(client['state'].type).toBe('connecting')
    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    expect(socket2.consumeMessage()).toBeUndefined()
  })

  test('2 sub, backoff, 1 unsub', () => {
    const { client, sockets } = newClient()

    const { sub: sub1, event: event1, established: est1, error: err1 } = subscribeWithMocks(client, 'default/foo')
    const { sub: sub2, event: event2, established: est2, error: err2 } = subscribeWithMocks(client, 'default/foo')

    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    socket1.acceptSubscribe('default/foo')

    expect(est1).toHaveBeenCalledOnce()
    expect(err1).not.toBeCalled()
    expect(est2).toHaveBeenCalledOnce()
    expect(err2).not.toBeCalled()

    socket1.close()
    expect(client['state'].type).toBe('backoff')
    sub2.unsubscribe()

    vi.runAllTimers()

    expect(client['state'].type).toBe('connecting')
    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    const subId2 = socket2.acceptSubscribe('default/foo')
    socket2.sendData(subId2, { foo: 123 })

    expect(event1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(event2).not.toBeCalled()
  })

  test('backoff eager unsub', () => {
    const { client, sockets } = newClient()

    client['connect']()
    const socket = sockets.get(0)
    socket.close()
    expect(client['state'].type).toBe('backoff')

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')
    expect(client['state'].type).toBe('backoff')

    sub.unsubscribe()
    expect(established).not.toBeCalled()
    expect(error).not.toBeCalled()
  })

  test('data after unsubscribe', () => {
    const { client, sockets } = newClient()

    const { sub, event, established, error } = subscribeWithMocks(client, 'default/foo')
    
    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')
    sub.unsubscribe()

    socket.sendData(subId, { foo: 123 })

    expect(event).not.toBeCalled()
    expect(established).toHaveBeenCalledOnce()
    expect(error).not.toBeCalled()

    socket.acceptUnsubscribe(subId)

    expect(event).not.toBeCalled()
  })

  test('data after unsubscribe - after unsubscribe_success', () => {
    const { client, sockets } = newClient()

    const { sub, event, established, error } = subscribeWithMocks(client, 'default/foo')
    
    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')
    sub.unsubscribe()
    expect(event).not.toBeCalled()

    expect(established).toHaveBeenCalledOnce()
    expect(error).not.toBeCalled()

    socket.acceptUnsubscribe(subId)

    socket.sendData(subId, { foo: 123 })

    expect(event).not.toBeCalled()
  })

  test('unsubscribe error', (t) => {
    expectUncaughtException('[aws-appsync-events bug] unsubscribe error: UnknownOperationError (Unknown operation id)')
    const { client, sockets } = newClient()

    const { sub, event, established, error } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')

    sub.unsubscribe()

    expect(established).toHaveBeenCalledOnce()
    expect(error).not.toBeCalled()

    socket.consumeUnsubscribeRequest(subId)
    expect(event).not.toBeCalled()

    socket.send({
      type: 'unsubscribe_error',
      id: subId,
      errors: [
        {
          errorType: 'UnknownOperationError',
          message: 'Unknown operation id'
        }
      ]
    })
  })

  test.each([false, true])('disconnect - connecting, err - %s', (errBeforeClose) => {
    function failSocket(socket: Socket) {
      if (errBeforeClose) {
        socket.error()
      }
      socket.close()
    }
    const { client, sockets } = newClient()

    subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)

    expect(client['state'].type).toBe('connecting')
    failSocket(socket)

    expect(client['state']).toEqual({ type: 'backoff', attempt: 1 })
    vi.runAllTimers()

    const socket2 = sockets.get(1)
    socket2.open()
    expect(client['state'].type).toBe('handshaking')
    expect(socket2.consumeMessage()).toEqual({ type: 'connection_init' })
    failSocket(socket2)

    expect(client['state']).toEqual({ type: 'backoff', attempt: 2 })
    vi.runAllTimers()

    const socket3 = sockets.get(2)
    socket3.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    failSocket(socket3)

    expect(client['state']).toEqual({ type: 'backoff', attempt: 3 })
    socket3.consumeSubscribeRequest('default/foo')
    vi.runAllTimers()

    const socket4 = sockets.get(3)
    socket4.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    failSocket(socket4)

    expect(client['state'].type).toBe('failed')
    socket4.consumeSubscribeRequest('default/foo')
  })

  test('timeout retry', () => {
    const { client, sockets } = newClient({ retryBehavior: exponentialBackoffRetryBehavior(1) })

    client['connect']()
    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    expect(client['state'].type).toBe('connected')

    vi.runAllTimers()
    // backoff timeout also called as part of runAllTimers
    expect(client['state'].type).toBe('connecting')

    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    
    vi.runAllTimers()
    expect(client['state'].type).toBe('failed')
  })

  test('keepalive timeout', () => {
    const { client, sockets } = newClient({ retryBehavior: exponentialBackoffRetryBehavior(0) })

    client['connect']()
    const socket1 = sockets.get(0)
    socket1.openAndHandshake(100)
    expect(client['state'].type).toBe('connected')

    vi.advanceTimersByTime(75)
    socket1.send({ type: 'ka' })
    vi.advanceTimersByTime(75)
    expect(client['state'].type).toBe('connected')

    vi.runAllTimers()
    expect(client['state'].type).toBe('failed')
  })

  test('subs renewed on retry', () => {
    const { client, sockets } = newClient()

    const { established, error } = subscribeWithMocks(client, 'default/foo')
    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    const subId1 = socket1.acceptSubscribe('default/foo')

    expect(established).toHaveBeenCalledOnce()
    expect(error).not.toBeCalled()
    
    socket1.close()
    expect(client['state'].type).toBe('backoff')

    vi.runAllTimers()
    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    const subId2 = socket2.consumeSubscribeRequest('default/foo')

    expect(subId2).toBe(subId1)
  })

  test('onStateChanged - connected', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged })

    client['connect']()
    const socket = sockets.get(0)
    socket.open()
    expect(onStateChanged).not.toBeCalled()
    socket.handshake()
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('connected')
  })

  test('onStateChanged - backoff', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged })

    client['connect']()
    const socket = sockets.get(0)
    socket.close()
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('backoff')
  })

  test('onStateChanged - failed', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged, retryBehavior: () => -1 })

    client['connect']()
    const socket = sockets.get(0)
    socket.close()
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('failed')
  })

  test('multiple established', () => {
    const { client, sockets } = newClient()

    const { established } = subscribeWithMocks(client, 'default/foo')
    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    socket1.acceptSubscribe('default/foo')

    expect(established).toHaveBeenCalledOnce()
    established.mockClear()

    socket1.close()
    vi.runAllTimers()

    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    socket2.acceptSubscribe('default/foo')
    expect(established).toHaveBeenCalledOnce()
  })

  test('connected, unsub, backoff', () => {
    const { client, sockets } = newClient()

    const { sub } = subscribeWithMocks(client, 'default/foo')
    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    const subId = socket1.acceptSubscribe('default/foo')
    sub.unsubscribe()
    socket1.consumeUnsubscribeRequest(subId)

    socket1.close()
    expect(client['state'].type).toBe('backoff')

    vi.runAllTimers()
    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    expect(socket2.consumeMessage()).toBeUndefined()
  })
})
