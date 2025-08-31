import { expect, describe, test, vi, afterEach, beforeEach, onTestFinished } from 'vitest'
import { setTimeout as sleep } from 'node:timers/promises'
import { Client, exponentialBackoffRetryBehavior, parseEndpoint, ResettableTimer, type ClientOpts, type WebSocketAdapter, type WebSocketAdapterConstructor } from '../src/client.js'
import * as util from 'node:util'

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

describe('Client', () => {
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

  test('basic subscribe', async () => {
    const { client, sockets } = newClient()

    const next = vi.fn()
    const subThen = vi.fn()
    const subCatch = vi.fn()
    const sub = client.subscribe('default/foo', { next })

    void sub.promise.then(subThen, subCatch)

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.consumeSubscribeRequest('default/foo')

    await sleep(0)
    expect(subThen).not.toBeCalled()
    expect(subCatch).not.toBeCalled()

    socket.send({ type: 'subscribe_success', id: subId })

    await expect(sub.promise).resolves.not.toThrow()

    socket.sendData(subId, { foo: 123 })

    expect(next).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
  })

  test('subscribe - connecting', async () => {
    const { client, sockets } = newClient()

    const next1 = vi.fn()
    const then1 = vi.fn()
    const err1 = vi.fn()
    const sub1 = client.subscribe('default/foo', { next: next1 })
    void sub1.promise.then(then1, err1)

    expect(client['state'].type).toBe('connecting')

    const next2 = vi.fn()
    const then2 = vi.fn()
    const err2 = vi.fn()
    const sub2 = client.subscribe('default/foo', { next: next2 })
    void sub2.promise.then(then2, err2)

    const socket = sockets.get(0)
    socket.openAndHandshake()

    const subId = socket.acceptSubscribe('default/foo')

    await sleep(0)
    expect(then1).toBeCalled()
    expect(err1).not.toBeCalled()
    expect(then2).toBeCalled()
    expect(err2).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })

    expect(next1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(next2).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
  })

  test('subscribe - handshaking', async () => {
    const { client, sockets } = newClient()

    const next1 = vi.fn()
    const then1 = vi.fn()
    const err1 = vi.fn()
    const sub1 = client.subscribe('default/foo', { next: next1 })
    void sub1.promise.then(then1, err1)

    const socket = sockets.get(0)
    socket.open()
    expect(client['state'].type).toBe('handshaking')

    const next2 = vi.fn()
    const then2 = vi.fn()
    const err2 = vi.fn()
    const sub2 = client.subscribe('default/foo', { next: next2 })
    void sub2.promise.then(then2, err2)

    socket.handshake()

    const subId = socket.acceptSubscribe('default/foo')

    await sleep(0)
    expect(then1).toBeCalled()
    expect(err1).not.toBeCalled()
    expect(then2).toBeCalled()
    expect(err2).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })

    expect(next1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(next2).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
  })

  test('subscribe - connected', async () => {
    const { client, sockets } = newClient()

    const next1 = vi.fn()
    const then1 = vi.fn()
    const err1 = vi.fn()
    const sub1 = client.subscribe('default/foo', { next: next1 })
    void sub1.promise.then(then1, err1)

    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    const subId = socket.acceptSubscribe('default/foo')

    await sleep(0)
    expect(then1).toBeCalled()
    expect(err1).not.toBeCalled()

    const next2 = vi.fn()
    const then2 = vi.fn()
    const err2 = vi.fn()
    const sub2 = client.subscribe('default/foo', { next: next2 })
    void sub2.promise.then(then2, err2)

    await sleep(0)
    expect(then2).toBeCalled()
    expect(err2).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })

    expect(next1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(next2).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
  })

  test('subscribe - connected - new channel', async () => {
    const { client, sockets } = newClient()

    const next1 = vi.fn()
    const then1 = vi.fn()
    const err1 = vi.fn()
    const sub1 = client.subscribe('default/foo', { next: next1 })
    void sub1.promise.then(then1, err1)

    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(client['state'].type).toBe('connected')
    const subId1 = socket.acceptSubscribe('default/foo')

    await sleep(0)
    expect(then1).toBeCalled()
    expect(err1).not.toBeCalled()

    const next2 = vi.fn()
    const then2 = vi.fn()
    const err2 = vi.fn()
    const sub2 = client.subscribe('default/bar', { next: next2 })
    void sub2.promise.then(then2, err2)

    await sleep(0)
    expect(then2).not.toBeCalled()
    expect(err2).not.toBeCalled()

    const subId2 = socket.acceptSubscribe('default/bar')

    socket.sendData(subId1, { foo: 123 })

    expect(next1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(next2).not.toBeCalled()

    next1.mockClear()
    socket.sendData(subId2, { bar: 123 })

    expect(next1).not.toBeCalled()
    expect(next2).toHaveBeenCalledExactlyOnceWith({ bar: 123 })
  })

  test('subscribe - backoff', async () => {
    const { client, sockets } = newClient()

    client['connect']()
    const socket1 = sockets.get(0)
    socket1.close()
    expect(client['state'].type).toBe('backoff')

    const next = vi.fn()
    const then = vi.fn()
    const err = vi.fn()
    const sub = client.subscribe('default/foo', { next })

    void sub.promise.then(then, err)

    await sleep(0)
    expect(then).not.toBeCalled()
    expect(err).not.toBeCalled()

    vi.runAllTimers()
    expect(client['state'].type).toBe('connecting')
    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    socket2.acceptSubscribe('default/foo')

    await sleep(0)
    expect(then).toBeCalled()
    expect(err).not.toBeCalled()
    expect(next).not.toBeCalled()
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
    const onSilentError = vi.fn()
    const { client, sockets } = newClient({ onSilentError })

    client['connect']()
    const socket = sockets.get(0)
    socket.open()
    expect(socket.consumeMessage()).toEqual({ type: 'connection_init' })
    expect(client['state'].type).toBe('handshaking')
    socket.subscribeSuccess('default/foo')

    expect(onSilentError).toHaveBeenCalledOnce()
    const err = onSilentError.mock.lastCall![0] 
    expect(err).toBeInstanceOf(Error)
    expect(err.message).toMatch('handshake error: expected "connection_ack" but got "subscribe_success"')
  })

  test('unexpected binary message', () => {
    const onSilentError = vi.fn()
    const { client, sockets } = newClient({ onSilentError })

    client['connect']()
    const socket = sockets.get(0)
    socket.adapter.dispatchEvent(new MessageEvent('message', {
      data: new ArrayBuffer(0),
    }))
    expect(onSilentError).toHaveBeenCalledOnce()
    const err = onSilentError.mock.lastCall![0] 
    expect(err).toBeInstanceOf(Error)
    expect(err.message).toMatch('unexpected binary data in message')
  })

  test('unknown error', async () => {
    const onUncaughtException = vi.fn()
    process.once('uncaughtException', onUncaughtException)
    onTestFinished(() => void process.removeListener('uncaughtException', onUncaughtException))
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
    await sleep(0)
    expect(onUncaughtException).toHaveBeenCalledOnce()
    const err = onUncaughtException.mock.lastCall![0] 
    expect(err).toBeInstanceOf(Error)
    expect(err.message).toMatch('Unknown error: UnsupportedOperation (Operation not supported through the realtime channel)')
  })

  test('unknown message', async () => {
    const onSilentError = vi.fn()
    const { client, sockets } = newClient({ onSilentError })

    client['connect']()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.send({
      type: 'foo',
      bar: 123,
    })

    expect(onSilentError).toHaveBeenCalledOnce()
    const err = onSilentError.mock.lastCall![0] 
    expect(err).toBeInstanceOf(Error)
    expect(err.message).toMatch('unknown message: {"type":"foo","bar":123}')
  })

  test('unknown message - without type', async () => {
    const onSilentError = vi.fn()
    const { client, sockets } = newClient({ onSilentError })

    client['connect']()
    const socket = sockets.get(0)
    socket.openAndHandshake()
    socket.send({
      bar: 123,
    })

    expect(onSilentError).toHaveBeenCalledOnce()
    const err = onSilentError.mock.lastCall![0] 
    expect(err).toBeInstanceOf(Error)
    expect(err.message).toMatch('unknown message: {"bar":123}')
  })

  test('subscribe error', async () => {
    const { client, sockets } = newClient()

    const next = vi.fn()
    const sub = client.subscribe('default/foo', { next })

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

    await expect(sub.promise).rejects.toThrow('Subscribe error: UnauthorizedException, BadRequestException (Invalid Channel Format)')
  })

  test('unsubscribe - connecting', async () => {
    const { client, sockets } = newClient()

    const then = vi.fn()
    const err = vi.fn()
    const sub = client.subscribe('default/foo', {})

    void sub.promise.then(then, err)
    
    expect(client['state'].type).toBe('connecting')
    sub.unsubscribe()

    await sleep(0)

    expect(then).toBeCalled()
    expect(err).not.toBeCalled()

    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(socket.consumeMessage()).toBeUndefined()
  })

  test('double unsubscribe - connecting', async () => {
    const { client, sockets } = newClient()

    const then = vi.fn()
    const err = vi.fn()
    const sub = client.subscribe('default/foo', {})

    void sub.promise.then(then, err)
    
    expect(client['state'].type).toBe('connecting')
    sub.unsubscribe()
    sub.unsubscribe()

    await sleep(0)

    expect(then).toBeCalled()
    expect(err).not.toBeCalled()

    const socket = sockets.get(0)
    socket.openAndHandshake()
    expect(socket.consumeMessage()).toBeUndefined()
  })


  test('unsubscribe - handshaking', async () => {
    const { client, sockets } = newClient()

    const then = vi.fn()
    const err = vi.fn()
    const sub = client.subscribe('default/foo', {})

    void sub.promise.then(then, err)

    const socket = sockets.get(0)
    socket.open()
    
    expect(client['state'].type).toBe('handshaking')
    sub.unsubscribe()

    socket.handshake()

    await sleep(0)

    expect(then).toBeCalled()
    expect(err).not.toBeCalled()

    expect(socket.consumeMessage()).toBeUndefined()
  })

  test('unsubscribe - connecting - two subs', async () => {
    const { client, sockets } = newClient()

    const next1 = vi.fn()
    const then1 = vi.fn()
    const err1 = vi.fn()
    const sub1 = client.subscribe('default/foo', { next: next1 })
    void sub1.promise.then(then1, err1)

    const next2 = vi.fn()
    const sub2 = client.subscribe('default/foo', { next: next2 })
    
    expect(client['state'].type).toBe('connecting')
    sub2.unsubscribe()

    await expect(sub2.promise).resolves.not.toThrow()
    expect(then1).not.toBeCalled()
    expect(err1).not.toBeCalled()

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')

    await sleep(0)
    expect(then1).toBeCalled()
    expect(err1).not.toBeCalled()

    socket.sendData(subId, { foo: 123 })

    expect(next1).toHaveBeenCalledExactlyOnceWith({ foo: 123 })
    expect(next2).not.toBeCalled()
  })

  test('unsubscribe - connected - not subscribed', async () => {
    const { client, sockets } = newClient()

    const then = vi.fn()
    const err = vi.fn()
    const sub = client.subscribe('default/foo', {})

    void sub.promise.then(then, err)

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.consumeSubscribeRequest('default/foo')

    sub.unsubscribe()
    await sleep(0)

    expect(then).not.toBeCalled()
    expect(err).not.toBeCalled()
    expect(socket.consumeMessage()).toBeUndefined()

    socket.subscribeSuccess(subId)

    await sleep(0)
    expect(then).toBeCalled()
    expect(err).not.toBeCalled()
    socket.acceptUnsubscribe(subId)
  })

  test('unsubscribe - connected - subscribed', async () => {
    const { client, sockets } = newClient()

    const then = vi.fn()
    const err = vi.fn()
    const sub = client.subscribe('default/foo', {})

    void sub.promise.then(then, err)

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')

    await sleep(0)
    expect(then).toBeCalled()
    expect(err).not.toBeCalled()

    sub.unsubscribe()

    await sleep(0)
    socket.acceptUnsubscribe(subId)
  })

  test('unsubscribe - connected - failed', async () => {
    const { client, sockets } = newClient({ retryBehavior: exponentialBackoffRetryBehavior(0) })

    const then = vi.fn()
    const err = vi.fn()
    const sub = client.subscribe('default/foo', {})

    void sub.promise.then(then, err)

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')

    sub.unsubscribe()
    vi.runAllTimers()
    expect(client['state'].type).toBe('failed')

    await sleep(0)
    expect(then).toBeCalled()
    expect(err).not.toBeCalled()
  })

  test('unsubscribe - backoff', async () => {
    const { client, sockets } = newClient()

    const next = vi.fn()
    const then = vi.fn()
    const err = vi.fn()
    const sub = client.subscribe('default/foo', { next })

    void sub.promise.then(then, err)

    const socket1 = sockets.get(0)
    socket1.close()
    expect(client['state'].type).toBe('backoff')
    sub.unsubscribe()

    await sleep(0)
    expect(then).toBeCalled()
    expect(err).not.toBeCalled()

    vi.runAllTimers()
    expect(client['state'].type).toBe('connecting')
    const socket2 = sockets.get(1)
    socket2.openAndHandshake()
    expect(socket2.consumeMessage()).toBeUndefined()
  })

  test('data after unsubscribe', async () => {
    const { client, sockets } = newClient()

    const next = vi.fn()
    const sub = client.subscribe('default/foo', { next })
    
    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')
    sub.unsubscribe()

    socket.sendData(subId, { foo: 123 })

    expect(next).not.toBeCalled()
    await expect(sub.promise).resolves.not.toThrow()

    socket.acceptUnsubscribe(subId)


    await sleep(0)
    expect(next).not.toBeCalled()
  })

  test('data after unsubscribe - after unsubscribe_success', async () => {
    const { client, sockets } = newClient()

    const next = vi.fn()
    const sub = client.subscribe('default/foo', { next })
    
    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')
    sub.unsubscribe()
    expect(next).not.toBeCalled()
    await expect(sub.promise).resolves.not.toThrow()
    socket.acceptUnsubscribe(subId)

    socket.sendData(subId, { foo: 123 })

    await sleep(0)
    expect(next).not.toBeCalled()
  })

  test('unsubscribe error', async () => {
    const onSilentError = vi.fn()
    const { client, sockets } = newClient({ onSilentError })

    const next = vi.fn()
    const sub = client.subscribe('default/foo', {})

    const socket = sockets.get(0)
    socket.openAndHandshake()
    const subId = socket.acceptSubscribe('default/foo')

    sub.unsubscribe()
    await expect(sub.promise).resolves.not.toThrow()

    socket.consumeUnsubscribeRequest(subId)
    expect(next).not.toBeCalled()

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

    expect(onSilentError).toHaveBeenCalledOnce()
    const err = onSilentError.mock.lastCall![0] 
    expect(err).toBeInstanceOf(Error)
    expect(err.message).toMatch('Unsubscribe error: UnknownOperationError (Unknown operation id)')
  })

  test.each([false, true])('disconnect - connecting, err - %s', async (errBeforeClose) => {
    function failSocket(socket: Socket) {
      if (errBeforeClose) {
        socket.error()
      }
      socket.close()
    }
    const { client, sockets } = newClient()

    const then = vi.fn()
    const err = vi.fn()
    const sub = client.subscribe('default/foo', {})

    void sub.promise.then(then, err)

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

  test('timeout retry', async () => {
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

  test('keepalive timeout', async () => {
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
})
