const provisionedEndpointRe = /^(\w{26}\.(appsync|appsync-realtime)-api\.\w{2}(?:(?:-\w{2,})+)-\d\.amazonaws.com(?:\.cn)?)(?:\/event(?:\/realtime)?)?$/i
const protocolRe = /^(?:(?:(?:https?)|(?:wss?)):\/\/)?/i
const pathnameRe = /(?:\/(?:event(?:\/(?:realtime\/?)?)?)?)?$/i

export function parseEndpoint(endpoint: string): { http: string; realtime: string } {
  const domainName = endpoint
    .replace(protocolRe, '')
    .replace(pathnameRe, '')

  const match = provisionedEndpointRe.exec(domainName)
  if (match != null) {
    return {
      http: match[1]!.replace(match[2]!, 'appsync'),
      realtime: match[1]!.replace(match[2]!, 'appsync-realtime'),
    }
  }

  // throws TypeError if domainName is invalid
  // new URL() normalizes the domain name by converting it to Punycode
  const { hostname } = new URL(`http://${domainName}`)

  return {
    http: hostname,
    realtime: hostname,
  }
}

const leadingSlashRe = /^\/?/
const trailingSlashRe = /\/?$/
function normalizeChannel(channel: string) {
  return channel.replace(leadingSlashRe, '').replace(trailingSlashRe, '')
}

function normalizeErrors(errors: unknown) {
  return (Array.isArray(errors)
                ? (`: ` + errors.map((e: any) => String(e?.errorType) + (e?.message != null ? ` (${String(e.message)})` : '')).join(', '))
                : '')
}

interface PromiseWithResolvers<T> {
    promise: Promise<T>;
    resolve: (value: T | PromiseLike<T>) => void;
    reject: (reason?: any) => void;
}

export function promiseWithResolversPolyfill<T>(): PromiseWithResolvers<T> {
  let resolve!: PromiseWithResolvers<T>['resolve'], reject!: PromiseWithResolvers<T>['reject']
  return { 
    promise: new (Promise as PromiseConstructor)<T>((res, rej) => (resolve = res, reject = rej)), 
    resolve, 
    reject,
  }
}
const promiseWithResolvers: <T>() => PromiseWithResolvers<T> = (Promise as any).withResolvers?.bind(Promise) ?? promiseWithResolversPolyfill

type ApiKeyAuthorization = {
  type: 'API_KEY'
  key: string
}

type Authorization = ApiKeyAuthorization

type Listener = (event: unknown) => void

export type ClientState = 'connected' | 'backoff' | 'failed'
export type ClientOpts = {
  /**
   * Function that determines the delay between retry attempts.
   *
   * Defaults to **exponential backoff with jitter** (see
   * {@link https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/ | AWS blog}),
   * up to **3 attempts**.
   * 
   * @param attempt The current retry attempt number (starting from 1).
   * @returns The delay in milliseconds before the next retry attempt,
   * or -1 to stop retrying and treat the connection as failed.
   */
  retryBehavior?: ((attempt: number) => number) | undefined

  onStateChanged?: ((newState: ClientState) => void) | undefined
}

// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
export function exponentialBackoffRetryBehavior(maxAttempts: number): (attempt: number) => number {
  return attempt => attempt > maxAttempts
    ? -1
    : Math.min(Math.random() * (2 ** attempt), 20) * 1_000
}

export class ResettableTimer {
  readonly timeoutMs: number
  readonly onTimeout: () => void
  timerId: ReturnType<typeof setTimeout> | null = null

  constructor(timeoutMs: number, onTimeout: () => void) {
    this.timeoutMs = timeoutMs
    this.onTimeout = onTimeout

    this.reset()
  }

  reset = (): void => {
    this.cancel()
    this.timerId = setTimeout(this.onTimeout, this.timeoutMs)
  }

  cancel = (): void => {
    if (this.timerId != null) {
      clearTimeout(this.timerId)
    }
  }
}

export type WebSocketAdapter = {
  addEventListener: <K extends keyof WebSocketEventMap>(type: K, listener: (ev: WebSocketEventMap[K]) => any) => void
  removeEventListener: <K extends keyof WebSocketEventMap>(type: K, listener: (ev: WebSocketEventMap[K]) => any) => void
  send: (data: string) => void
  close: () => void
}
export type WebSocketAdapterConstructor = {
  new (url: string, protocols: string[]): WebSocketAdapter
}

export class Client {
  readonly httpEndpoint: string
  readonly realtimeEndpoint: string
  private wsCtor: WebSocketAdapterConstructor = WebSocket

  private readonly subsByChannelName: Map<string, {
    id: string
    promise: Promise<void>
    /** Calls to resolve are idempodent */
    resolve: () => void
    /** Calls to reject are idempodent */
    reject: (e: Error) => void
    listeners: Set<Listener>
    subscribeRequestSent: boolean
  }> = new Map()
  private readonly channelNamesById: Map<string, string> = new Map()
  private subById = (id: string) => {
    const channel = this.channelNamesById.get(id)
    if (channel == null) {
      return null
    }
    return this.subsByChannelName.get(channel)!
  }

  private readonly retryBehavior: (attempt: number) => number
  private readonly onStateChanged: ((newState: ClientState) => void) | null

  private state: {
    type: 'idle'
  } | {
    type: 'connecting'
    ws: WebSocketAdapter
  } | {
    type: 'handshaking'
    ws: WebSocketAdapter
  } | {
    type: 'connected'
    ws: WebSocketAdapter
    timeoutTimer: ResettableTimer
  } | {
    type: 'backoff'
    attempt: number
  } | {
    type: 'failed'
  } = {
    type: 'idle',
  }

  constructor(endpoint: string, readonly authorization: Authorization, {
    retryBehavior = exponentialBackoffRetryBehavior(3),
    onStateChanged,
  }: ClientOpts = {}) {
    const endpoints = parseEndpoint(endpoint)
    this.httpEndpoint = endpoints.http
    this.realtimeEndpoint = endpoints.realtime
    this.retryBehavior = retryBehavior
    this.onStateChanged = onStateChanged ?? null
  }

  subscribe = (channel: string, { next, error }: {
    // TODO: make required?
    next?: (event: unknown) => void
    error?: ((err: Error) => void) | undefined
  }) => {
    channel = normalizeChannel(channel)
    let sub = this.subsByChannelName.get(channel)
    let firstSub = false
    if (sub == null) {
      firstSub = true
      this.subsByChannelName.set(channel, sub = {
        id: crypto.randomUUID(),
        listeners: new Set(),
        subscribeRequestSent: false,
        ...promiseWithResolvers<void>(),
      })
    }

    const eagerUnsub = promiseWithResolvers<void>()

    const listener: Listener = event => {
      next?.(event)
    }
    sub.listeners.add(listener)
    this.channelNamesById.set(sub.id, channel)

    switch (this.state.type) {
      case 'idle':
        this.connect()
      case 'connecting':
      case 'backoff':
      case 'handshaking':
        break
      case 'connected':
        if (firstSub) {
          sub.subscribeRequestSent = true
          this.state.ws.send(JSON.stringify({
            type: 'subscribe',
            id: sub.id,
            channel,
            authorization: this.authorizationHeaders,
          }))
        }
        break
      case 'failed': // TODO
    }

    let unsubscribed = false
    const unsubscribe = () => {
      if (unsubscribed) {
        return
      }
      unsubscribed = true

      sub.listeners.delete(listener)

      const tryCleanupListeners = () => {
        if (sub.listeners.size === 0) {
          // TODO: should we close connection?
          this.subsByChannelName.delete(channel)
          this.channelNamesById.delete(sub.id)
        }
      }

      switch (this.state.type) {
        // case 'idle' - should never happen: unsubscribe can be called only
        // after first call to connect
        case 'backoff':
          // If the state went out of 'connected', we have no reason to send
          // unsubscribe request. All subscriptions are cancelled since WebSocket
          // is closed.
        case 'connecting':
        case 'handshaking':
          tryCleanupListeners()
          eagerUnsub.resolve()
          break
        case 'connected':
          // We don't handle rejection, because rejection of this promise
          // indicates subscribe_error. In that case, we don't need to
          // explicitly unsubscribe.
          void sub.promise.then(() => {
            // We defer cleanup until after sub.promise, because subscribe_success
            // should find the current sub and call sub.resolve()
            tryCleanupListeners()
            if (this.state.type !== 'connected') {
              return
            }
            // If unsubscribe is sent at the same time as subscribe, it will be
            // silently ignored. This is why we wait for the promise to resolve
            // before unsubscribing.
            this.state.ws.send(JSON.stringify({
              type: 'unsubscribe',
              id: sub.id,
            }))
          })
          break
        case 'failed': // TODO
          break
      }
    }

    return {
      promise: Promise.race([sub.promise, eagerUnsub.promise]),
      unsubscribe: () => {
        unsubscribe()
      },
    }
  }

  private connect = () => {
    // TODO: should we still open connection if there is no active subs
    let attempt = 0
    switch (this.state.type) {
      case 'idle':
        break
      case 'connecting':
      case 'handshaking':
      case 'connected':
        return
      case 'backoff':
        attempt = this.state.attempt
        break
    }

    const authPayload = btoa(JSON.stringify(this.authorizationHeaders))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/g, '')
    // TODO: think about memory leaks
    // See https://websockets.spec.whatwg.org/#garbage-collection
    const ws = new this.wsCtor(`wss://${this.realtimeEndpoint}/event/realtime`, [
      'aws-appsync-event-ws',
      `header-${authPayload}`,
    ])

    this.state = {
      type: 'connecting',
      ws,
    }

    const onOpen = () => {
      ws.removeEventListener('open', onOpen)
      this.state = {
        type: 'handshaking',
        ws,
      }
      ws.send(JSON.stringify({ type: 'connection_init' }))
    }
    const onMessage = (event: MessageEvent) => {
      if (typeof event.data !== 'string') {
        throw new Error(`[aws-appsync-events bug] unexpected binary data in message: ${event.data}`)
      }

      // TODO: validate incoming data
      const message = JSON.parse(event.data)

      switch (this.state.type) {
        // case 'idle' - should never happen: the ws is created only after
        // entering the 'connecting' state
        // case 'connecting' - should never happen: message cannot be delivered
        // before the 'open' state
        // case 'backoff' - should never happen: the 'message' listener is
        // removed before leaving the 'connected' state
        // case 'failed' - should never happen: the 'message' listener is
        // removed before leaving the 'connected' state
        case 'handshaking':
          if (message.type !== 'connection_ack') {
            throw new Error(`[aws-appsync-events bug] handshake error: expected "connection_ack" but got ${JSON.stringify(message.type)}`)
          }
          this.state = {
            type: 'connected',
            ws,
            timeoutTimer: new ResettableTimer(message.connectionTimeoutMs, () => {
              ws.close()
              onClose()
            }),
          }
          for (const [channel, sub] of this.subsByChannelName) {
            sub.subscribeRequestSent = true
            ws.send(JSON.stringify({
              type: 'subscribe',
              id: sub.id,
              channel,
              authorization: this.authorizationHeaders,
            }))
          }
          this.onStateChanged?.('connected')
          break
        case 'connected':
          switch (message.type) {
            case 'ka':
              this.state.timeoutTimer.reset()
              break
            case 'subscribe_success': {
              const sub = this.subById(message.id)
              if (sub == null) {
                throw new Error(`[aws-appsync-events bug] subscribe_success for unknown sub`)
              }
              sub.resolve()
              break
            }
            case 'subscribe_error': {
              const sub = this.subById(message.id)
              if (sub == null) {
                throw new Error(`[aws-appsync-events bug] subscribe_error for unknown sub`)
              }
              const error = new Error('Subscribe error' + normalizeErrors(message.errors))
              sub.reject(error)
              break
            }
            case 'data': {
              const channel = this.channelNamesById.get(message.id)
              if (channel == null) {
                break
              }
              for (const listener of this.subsByChannelName.get(channel)!.listeners) {
                listener(JSON.parse(message.event))
              }
              break
            }
            case 'unsubscribe_success':
              break
            case 'unsubscribe_error':
              throw new Error('[aws-appsync-events bug] unsubscribe error' + normalizeErrors(message.errors))
            case 'error':
              throw new Error('[aws-appsync-events] unknown error' + normalizeErrors(message.errors))
            default:
              throw new Error(`[aws-appsync-events] unknown message: ${JSON.stringify(message)}`)
          }
      }
    }

    const onClose = () => {
      // should cancel all existing subs
      // TODO: handle manual close
      removeEventListeners()

      switch (this.state.type) {
        case 'connected':
          this.state.timeoutTimer.cancel()
        case 'idle':
        case 'connecting':
        case 'handshaking': {
          const newAttempt = attempt + 1
          const msToSleep = this.retryBehavior(newAttempt)
          if (msToSleep < 0) {
            this.state = { type: 'failed' }
            this.onStateChanged?.('failed')
            return
          }
          this.state = {
            type: 'backoff',
            attempt: newAttempt,
          }
          this.onStateChanged?.('backoff')
          setTimeout(this.connect, msToSleep)
          break
        }
        case 'backoff':
          // should never happen
      }
    }

    ws.addEventListener('open', onOpen)
    ws.addEventListener('message', onMessage)
    // We don't distinguish between 'error' and 'close' events since it's not
    // useful for our purposes. Older Node.js versions also do not call 'error'
    // in case of network failures (only 'close').
    // See https://github.com/nodejs/undici/issues/4487
    ws.addEventListener('error', onClose)
    ws.addEventListener('close', onClose)
    
    function removeEventListeners() {
      ws.removeEventListener('open', onOpen)
      ws.removeEventListener('message', onMessage)
      ws.removeEventListener('error', onClose)
      ws.removeEventListener('close', onClose)
    }
  }

  publish = async (channel: string, events: unknown[]) => {
    const response = await fetch(`https://${this.httpEndpoint}/event`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        ...this.authorizationHeaders,
      },
      body: JSON.stringify({
        channel,
        events: events.map(e => JSON.stringify(e)),
      })
    })
    const responseBodyText = await response.text()
    let res: unknown
    try {
      res = JSON.parse(responseBodyText)
    } catch {
      throw new Error(`Publish error: ${response.statusText} ${response.status}${responseBodyText}`)
    }
    if (typeof res !== 'object' || res == null) {
      return
    }
    if ('errors' in res && Array.isArray(res.errors) && res.errors.length > 0) {
      throw new Error(`Publish error: ${JSON.stringify(res.errors, null, 2)}`)
    }
    if ('failed' in res && Array.isArray(res.failed) && res.failed.length > 0) {
      throw new Error(`Publish error: ${JSON.stringify(res.failed, null, 2)}`)
    }
  }

  private get authorizationHeaders() {
    switch (this.authorization.type) {
      case 'API_KEY':
        return {
          host: this.httpEndpoint,
          'x-api-key': this.authorization.key,
        }
    }
  }
}
