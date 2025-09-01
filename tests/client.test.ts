import { expect, describe, test, vi, afterEach, beforeEach, onTestFinished, it } from 'vitest'
import { setTimeout as sleep } from 'node:timers/promises'
import { apiKeyAuthorizer, Client, parseEndpoint, ResettableTimer, type ClientOpts, type WebSocketAdapter, type WebSocketAdapterConstructor } from '../src/client.js'
import * as util from 'node:util'

function simpleRetryBehavior(maxAttempts: number, delay = 10) {
  return (attempt: number) => attempt > maxAttempts ? false : delay
}

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

  async function subscribeWithMocks(client: Client, channel: string) {
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

    consumeSubscribeRequest = (channel: string): string => {
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

    consumeConnectionInit = () => {
      expect(this.consumeMessage()).toEqual({ type: 'connection_init' })
    }

    handshake = (connectionTimeoutMs: number | undefined = 30_000 ) => {
      this.consumeConnectionInit()
      this.send({ type: 'connection_ack', connectionTimeoutMs })
    }
  }

  function newClient(opts?: ClientOpts) {
    const client = new Client('example.com', apiKeyAuthorizer('foo'), {
      retryBehavior: simpleRetryBehavior(3),
      ...opts,
    })
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
        expect(this.closed).toBeFalsy()
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
        count: () => sockets.length,
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

  it('should establish both listeners and deliver data when subscribing twice during connect', () => {
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

  it('should queue second subscription during handshake and establish both after ack', () => {
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

  it('should immediately establish the second same-channel subscription when already connected', () => {
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

  it('should subscribe to a new channel and isolate events by channel when connected', () => {
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

  it('should defer subscription until reconnect during backoff, then establish', () => {
    const { client, sockets } = newClient()

    client.connect()
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

  it('should reconnect when subscribing after a failure', () => {
    const { client, sockets } = newClient({ retryBehavior: () => false })

    client.connect()
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

  it('should ignore a late open from a previous socket after reconnect', () => {
    const { client, sockets } = newClient()

    subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.acceptSubscribe('default/foo')
    socket.close()
    expect(client['state'].type).toBe('backoff')

    vi.runAllTimers()
    expect(client['state'].type).toBe('connecting')
    socket.open()
    expect(socket.consumeMessage()).toBeUndefined()
  })

  it('should ignore a duplicate open after handshake and remain connected', () => {
    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.open()
    expect(client['state'].type).toBe('connected')
  })

  it('should throw a handshake error if a message arrives before connection_ack', () => {
    expectUncaughtException('handshake error: expected "connection_ack" but got "subscribe_success"')

    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.open()
    socket.consumeConnectionInit()
    expect(client['state'].type).toBe('handshaking')
    socket.subscribeSuccess('default/foo')
  })

  it('should throw on unexpected binary WebSocket message', () => {
    expectUncaughtException('unexpected binary data in message')
    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.adapter.dispatchEvent(new MessageEvent('message', {
      data: new ArrayBuffer(0),
    }))
  })

  it('should throw unknown error with details when server sends error frame', () => {
    expectUncaughtException('[aws-appsync-events] unknown error: UnsupportedOperation (Operation not supported through the realtime channel)')
    const { client, sockets } = newClient()

    client.connect()
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

  it('should throw on unknown message type', () => {
    expectUncaughtException('unknown message: {"type":"foo","bar":123}')
    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.send({
      type: 'foo',
      bar: 123,
    })

  })

  it('should throw on message missing a type', () => {
    expectUncaughtException('unknown message: {"bar":123}')
    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.send({
      bar: 123,
    })

  })

  it('should call error with details on subscribe_error', () => {
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

  it('should call error exactly once for a failed subscription and not resubscribe on reconnect', () => {
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

  it('should throw if subscribe_success references an unknown subscription id', () => {
    expectUncaughtException('[aws-appsync-events bug] subscribe_success for unknown sub')
    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.subscribeSuccess('foo')
  })

  it('should throw if subscribe_error references an unknown subscription id', () => {
    expectUncaughtException('[aws-appsync-events bug] subscribe_error for unknown sub')
    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.send({
      id: 'foo',
      type: 'subscribe_error',
    })
  })

  it('should ignore data for a subscription that was unsubscribed before it established', () => {
    const { client, sockets } = newClient()

    const { sub, event } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()
    sub.unsubscribe()
    const subId = socket.acceptSubscribe('default/foo')
    socket.sendData(subId, { foo: 123 })
    socket.acceptUnsubscribe(subId)

    expect(event).not.toBeCalled()
  })

  it('should cancel subscription if unsubscribed while connecting', () => {
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

  it('should treat double unsubscribe during connecting as idempotent', () => {
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


  it('should cancel pending subscription if unsubscribed during handshaking', () => {
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

  it('should keep first listener and drop second when second unsubscribes during connecting', () => {
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

  it('should queue unsubscribe issued before ack', () => {
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

    expect(established).not.toBeCalled()
    expect(error).not.toBeCalled()
    socket.acceptUnsubscribe(subId)
  })

  it('should send unsubscribe for an established subscription when connected', () => {
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

  it('should not emit error when unsubscribing from an established subscription even if the connection later fails', () => {
    const { client, sockets } = newClient({ retryBehavior: () => false, idleConnectionKeepAliveTimeMs: 20 })

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')

    const socket = sockets.get(0)
    socket.openAndHandshake(10)
    const subId = socket.acceptSubscribe('default/foo')

    sub.unsubscribe()
    socket.acceptUnsubscribe(subId)
    vi.runAllTimers()
    expect(client['state'].type).toBe('failed')

    expect(established).toBeCalled()
    expect(error).not.toBeCalled()
  })

  it('should not resubscribe after unsubscribing during backoff', () => {
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

  it('should resubscribe only remaining listener after one of two unsubscribes during backoff', () => {
    const { client, sockets } = newClient()

    const { event: event1, established: est1, error: err1 } = subscribeWithMocks(client, 'default/foo')
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

  it('should remove only the unsubscribed listener when multiple listeners exist on the same channel', () => {
    const { client, sockets } = newClient()

    const { established: est1, event: event1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.consumeSubscribeRequest('default/foo')

    const { sub: sub2, established: est2, event: event2 } = subscribeWithMocks(client, 'default/foo')
    sub2.unsubscribe()

    socket.subscribeSuccess(subId)
    expect(est1).toHaveBeenCalledOnce()
    expect(est2).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })
    expect(event1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(event2).not.toBeCalled()
  })

  it('should delay establishment of second listener until first subscription is acknowledged', () => {
    const { client, sockets } = newClient({})

    const { established: est1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.consumeSubscribeRequest('default/foo')

    const { established: est2 } = subscribeWithMocks(client, 'default/foo')

    expect(est1).not.toBeCalled()
    expect(est2).not.toBeCalled()

    socket.subscribeSuccess(subId)

    expect(est1).toHaveBeenCalledOnce()
    expect(est2).toHaveBeenCalledOnce()
  })

  it('should immediately establish second listener when first subscription is already acknowledged', () => {
    const { client, sockets } = newClient({})

    const { established: est1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.acceptSubscribe('default/foo')

    const { established: est2 } = subscribeWithMocks(client, 'default/foo')

    expect(est1).toHaveBeenCalledOnce()
    expect(est2).toHaveBeenCalledOnce()
  })

  it('should clear unfinished unsubscribes when socket disconnects and not resubscribe them', () => {
    const { client, sockets } = newClient()

    const { sub: sub1 } = subscribeWithMocks(client, 'default/foo')
    subscribeWithMocks(client, 'default/bar')
    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    sub1.unsubscribe()
    socket1.consumeSubscribeRequest('default/foo')
    socket1.consumeSubscribeRequest('default/bar')
    socket1.close()

    vi.runAllTimers()

    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    socket2.consumeSubscribeRequest('default/bar')
  })

  it('should renew a subscription if a new listener is added after an unfinished unsubscribe before reconnect', () => {
    const { client, sockets } = newClient()

    const { sub: sub1 } = subscribeWithMocks(client, 'default/foo')
    subscribeWithMocks(client, 'default/bar')
    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    sub1.unsubscribe()
    socket1.consumeSubscribeRequest('default/foo')
    socket1.consumeSubscribeRequest('default/bar')

    subscribeWithMocks(client, 'default/foo')

    socket1.close()

    vi.runAllTimers()

    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    socket2.consumeSubscribeRequest('default/foo')
    socket2.consumeSubscribeRequest('default/bar')
  })

  it('should allow unsubscribe during backoff without sending subscribe or firing callbacks', () => {
    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.close()
    expect(client['state'].type).toBe('backoff')

    const { sub, established, error } = subscribeWithMocks(client, 'default/foo')
    expect(client['state'].type).toBe('backoff')

    sub.unsubscribe()
    expect(established).not.toBeCalled()
    expect(error).not.toBeCalled()
  })

  it('should ignore events sent after unsubscribe request', () => {
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

  it('should ignore events sent after unsubscribe_success response', () => {
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

  it('should throw on unsubscribe_error', () => {
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

  it.each([false, true])('should retry with exponential backoff on disconnects and eventually fail after max attempts, err - %s', (errBeforeClose) => {
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

    expect(client['state']).toMatchObject({ type: 'backoff', attempt: 1 })
    vi.runAllTimers()

    const socket2 = sockets.get(1)
    socket2.open()
    expect(client['state'].type).toBe('handshaking')
    socket2.consumeConnectionInit()
    failSocket(socket2)

    expect(client['state']).toMatchObject({ type: 'backoff', attempt: 2 })
    vi.runAllTimers()

    const socket3 = sockets.get(2)
    socket3.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    failSocket(socket3)

    expect(client['state']).toMatchObject({ type: 'backoff', attempt: 3 })
    socket3.consumeSubscribeRequest('default/foo')
    vi.runAllTimers()

    const socket4 = sockets.get(3)
    socket4.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    failSocket(socket4)

    expect(client['state'].type).toBe('failed')
    socket4.consumeSubscribeRequest('default/foo')
  })

  it('should retry on timeout until max attempts are reached, then transition to failed', () => {
    const { client, sockets } = newClient({ retryBehavior: simpleRetryBehavior(1) })

    subscribeWithMocks(client, 'default/foo')
    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    socket1.acceptSubscribe('default/foo')
    expect(client['state'].type).toBe('connected')

    vi.runAllTimers()
    // backoff timeout also called as part of runAllTimers
    expect(client['state'].type).toBe('connecting')

    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    socket2.acceptSubscribe('default/foo')
    expect(client['state'].type).toBe('connected')
    
    vi.runAllTimers()
    expect(client['state'].type).toBe('failed')
  })

  it('should reset keepalive timer on ka messages and fail after timeout expires', () => {
    const { client, sockets } = newClient({ retryBehavior: simpleRetryBehavior(0) })

    client.connect()
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

  it('should resubscribe active channels after reconnect using the same subscription ids', () => {
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

  it('should invoke onStateChanged with connected after handshake completes', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged })

    client.connect()
    const socket = sockets.get(0)
    socket.open()
    expect(onStateChanged).not.toBeCalled()
    socket.handshake()
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('connected')
  })

  it('should invoke onStateChanged with backoff after socket closes and retry starts', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged })

    client.connect()
    const socket = sockets.get(0)
    socket.close()
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('backoff')
  })

  it('should invoke onStateChanged with failed when retries are disabled and socket closes', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged, retryBehavior: () => false })

    client.connect()
    const socket = sockets.get(0)
    socket.close()
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('failed')
  })

  it('should call established again when subscription is re-established after reconnect', () => {
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

  it('should not resubscribe after unsubscribe if connection drops and reconnects', () => {
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

  it('should transition to idle after idle keep-alive timeout', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged })

    client.connect()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    onStateChanged.mockClear()

    vi.runAllTimers()

    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('idle')
  })

  it.for([true, false])('should close idle connection during connecting (immediate %s)', (immediate) => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged, idleConnectionKeepAliveTimeMs: !immediate && 1 })

    const { sub } = subscribeWithMocks(client, 'default/foo')
    expect(client['state'].type).toBe('connecting')
    sub.unsubscribe()

    if (!immediate) {
      vi.runAllTimers()
    }

    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).not.toBeCalled()
  })

  it.for([true, false])('should close idle connection during handshaking (immediate %s)', (immediate) => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged, idleConnectionKeepAliveTimeMs: !immediate && 1 })

    const { sub } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.open()
    socket.consumeConnectionInit()
    expect(client['state'].type).toBe('handshaking')
    sub.unsubscribe()

    if (!immediate) {
      vi.runAllTimers()
    }

    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).not.toBeCalled()
  })

  it.for([true, false])('should close idle connection during connected (immediate %s)', (immediate) => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged, idleConnectionKeepAliveTimeMs: !immediate && 1 })

    const { sub } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()
    onStateChanged.mockClear()

    expect(client['state'].type).toBe('connected')
    const subId = socket.acceptSubscribe('default/foo')
    sub.unsubscribe()

    if (!immediate) {
      socket.consumeUnsubscribeRequest(subId)
      vi.runAllTimers()
    } else {
      // we don't expect unsubscribe message, because socket should be just closed
      expect(socket.consumeMessage()).toBeUndefined()
    }

    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('idle')
  })

  it('should reset idle timer when a new subscription is added before timeout', () => {
    const { client, sockets } = newClient({ retryBehavior: () => false, idleConnectionKeepAliveTimeMs: 20 })

    const { sub: sub1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')
    sub1.unsubscribe()
    socket.consumeUnsubscribeRequest(subId)

    vi.advanceTimersByTime(10)

    subscribeWithMocks(client, 'default/bar')
    vi.advanceTimersByTime(50)
    socket.acceptSubscribe('default/bar')
    expect(client['state'].type).toBe('connected')
  })

  it('should keep alive across connecting->handshaking when a new sub arrives before idle timeout', () => {
    const { client, sockets } = newClient({ idleConnectionKeepAliveTimeMs: 20 })

    const { sub: sub1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    expect(client['state'].type).toBe('connecting')
    sub1.unsubscribe()
    socket.open()

    vi.advanceTimersByTime(15)

    subscribeWithMocks(client, 'default/bar')

    socket.handshake()

    vi.advanceTimersByTime(15)

    expect(client['state'].type).toBe('connected')
    socket.consumeSubscribeRequest('default/bar')
  })

  it('should keep alive across handshaking->connected when a new sub arrives before idle timeout', () => {
    const { client, sockets } = newClient({ idleConnectionKeepAliveTimeMs: 20 })

    const { sub: sub1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.open()
    expect(client['state'].type).toBe('handshaking')
    sub1.unsubscribe()

    vi.advanceTimersByTime(15)

    socket.handshake()
    expect(client['state'].type).toBe('connected')

    subscribeWithMocks(client, 'default/bar')

    vi.advanceTimersByTime(15)

    socket.consumeSubscribeRequest('default/bar')
  })

  it('should reset previous idle timer when entring connected with 0 subs', () => {
    const { client, sockets } = newClient({ idleConnectionKeepAliveTimeMs: 20 })

    const { sub: sub1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.open()
    expect(client['state'].type).toBe('handshaking')
    sub1.unsubscribe()

    vi.advanceTimersByTime(15)

    socket.handshake()

    vi.advanceTimersByTime(15)

    expect(client['state'].type).toBe('connected')
  })

  it('should not open a connection when there are no subs and keepalive is disabled', () => {
    const onStateChanged = vi.fn()
    const { client } = newClient({ onStateChanged, idleConnectionKeepAliveTimeMs: false })

    client.connect()
    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).not.toBeCalled()
  })

  it.for(['backoff', 'failed'])('should not reopen connection from %s when there are no subs and keepalive is disabled', (fromState) => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged, retryBehavior: fromState === 'failed' ? () => false : () => 5, idleConnectionKeepAliveTimeMs: false })

    const { sub } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.acceptSubscribe('default/foo')
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('connected')
    onStateChanged.mockClear()

    socket.close()
    expect(client['state'].type).toBe(fromState)
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith(fromState)
    onStateChanged.mockClear()

    sub.unsubscribe()

    if (fromState === 'failed') {
      client.connect()
    } else {
      vi.runAllTimers()
    }

    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('idle')
  })

  it('should allow explicit reconnect during backoff', () => {
    const { client, sockets } = newClient({ retryBehavior: () => 10 })

    client.connect()
    const socket1 = sockets.get(0)
    socket1.close()
    expect(client['state']).toMatchObject({ type: 'backoff', attempt: 1 })
    vi.advanceTimersByTime(7)

    client.connect()
    expect(client['state'].type).toBe('connecting')
    const socket2 = sockets.get(1)
    socket2.close()
    expect(client['state']).toMatchObject({ type: 'backoff', attempt: 2 })
    vi.advanceTimersByTime(7)
    expect(client['state']).toMatchObject({ type: 'backoff', attempt: 2 })

    vi.advanceTimersByTime(7)
    sockets.get(2)
    expect(client['state'].type).toBe('connecting')
  })

  it('should ignore redundant connect call while already connecting', () => {
    const { client, sockets } = newClient()

    client.connect()
    expect(client['state'].type).toBe('connecting')
    client.connect()
    expect(sockets.count()).toBe(1)
  })

  it('should ignore redundant connect call while handshaking', () => {
    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.open()
    socket.consumeConnectionInit()
    expect(client['state'].type).toBe('handshaking')
    client.connect()
    expect(sockets.count()).toBe(1)
  })

  it('should ignore redundant connect call while connected', () => {
    const { client, sockets } = newClient()

    client.connect()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    client.connect()
    expect(sockets.count()).toBe(1)
  })

  it('should close manually during connecting and transition to idle', () => {
    const onStateChanged = vi.fn()
    const { client } = newClient({ onStateChanged })

    client.connect()
    expect(client['state'].type).toBe('connecting')
    client.close()
    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).not.toBeCalled()
  })

  it('should close manually during handshaking and transition to idle', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged })

    client.connect()
    const socket = sockets.get(0)
    socket.open()
    socket.consumeConnectionInit()
    expect(client['state'].type).toBe('handshaking')
    client.close()
    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).not.toBeCalled()
  })

  it('should close manually during connected and transition to idle', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged })

    client.connect()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(client['state'].type).toBe('connected')

    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('connected')
    onStateChanged.mockClear()

    client.close()

    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('idle')
  })

  it('should close manually during backoff, cancel retry, and transition to idle', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged, retryBehavior: () => 10 })

    client.connect()
    const socket = sockets.get(0)
    socket.close()
    expect(client['state'].type).toBe('backoff')

    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('backoff')
    onStateChanged.mockClear()

    client.close()

    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('idle')

    vi.runAllTimers()

    expect(client['state'].type).toBe('idle')
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('idle')
  })

  it('should ignore manual close during failed state and remain failed', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged, retryBehavior: () => false })

    client.connect()
    const socket = sockets.get(0)
    socket.close()
    expect(client['state'].type).toBe('failed')

    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('failed')
    onStateChanged.mockClear()

    client.close()

    expect(client['state'].type).toBe('failed')
    expect(onStateChanged).not.toBeCalled()
  })

  it('should clear pending unsubs on subscribe_error so disconnect does not crash', () => {
    const { client, sockets } = newClient()
  
    const { error: err1, established: est1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.consumeSubscribeRequest('default/foo')
  
    const { sub: sub2, error: err2 } = subscribeWithMocks(client, 'default/foo')
    sub2.unsubscribe()
  
    socket.send({
      type: 'subscribe_error',
      id: subId,
      errors: [{ errorType: 'UnauthorizedException' }],
    })
  
    expect(est1).not.toBeCalled()
    expect(err1).toHaveBeenCalledTimes(1)
    expect(err2).toHaveBeenCalledTimes(1)
  
    socket.close()
    expect(client['state'].type).toBe('backoff')
  })

  it('should not crash in connectionEnded when mappings were removed by error callbacks', () => {
    const { client, sockets } = newClient()
  
    const { sub: sub1, error: err1 } = subscribeWithMocks(client, 'default/foo')
    const { sub: sub2 } = subscribeWithMocks(client, 'default/foo')
  
    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.consumeSubscribeRequest('default/foo')
  
    sub2.unsubscribe()
  
    err1.mockImplementation(() => {
      sub1.unsubscribe()
    })
  
    socket.send({
      type: 'subscribe_error',
      id: subId,
      errors: [{ errorType: 'UnauthorizedException' }],
    })
  
    expect(() => socket.close()).not.toThrow()
    expect(client['state'].type).toBe('backoff')
  })

  it('should call established once when sub initiated in established cb', () => {
    const { client, sockets } = newClient()

    const { established: est1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()

    let est2!: typeof est1
    est1.mockImplementation(() => ({ established: est2 } = subscribeWithMocks(client, 'default/foo')))

    socket.acceptSubscribe('default/foo')

    expect(est2).toHaveBeenCalledOnce()
  })

  it('should call error for subs initiated in error cb', () => {
    const { client, sockets } = newClient()

    const { error: err1 } = subscribeWithMocks(client, 'default/foo')
    const socket = sockets.get(0)
    socket.openAndHandshake()

    let err2!: typeof err1
    err1.mockImplementation(() => ({ error: err2 } = subscribeWithMocks(client, 'default/foo')))

    const subId = socket.consumeSubscribeRequest('default/foo')
    socket.send({
      type: 'subscribe_error',
      id: subId,
      errors: [ { errorType: 'UnauthorizedException' }],
    })

    expect(err2).toHaveBeenCalledOnce()
  })

  it('should reset isEstablished on connection loss', () => {
    const { client, sockets } = newClient()

    const { sub } = subscribeWithMocks(client, 'default/foo')
    const socket1 = sockets.get(0)
    socket1.openAndHandshake()
    socket1.acceptSubscribe('default/foo')

    socket1.close()
    vi.runAllTimers()

    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    const subId = socket2.consumeSubscribeRequest('default/foo')

    sub.unsubscribe()

    socket2.subscribeSuccess(subId)
    socket2.consumeUnsubscribeRequest(subId)
  })

  it('should timeout handshake', () => {
    const onStateChanged = vi.fn()
    const { client, sockets } = newClient({ onStateChanged })

    client.connect()
    const socket = sockets.get(0)
    socket.open()
    socket.consumeConnectionInit()
    expect(client['state'].type).toBe('handshaking')

    vi.advanceTimersByTime(14_999)
    expect(onStateChanged).not.toBeCalled()

    vi.advanceTimersByTime(1)
    expect(onStateChanged).toHaveBeenCalledExactlyOnceWith('backoff')
  })
})
