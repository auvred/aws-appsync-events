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

type ApiKeyAuthorization = {
  type: 'API_KEY'
  key: string
}

type Authorization = ApiKeyAuthorization

type SubListener = {
  event: (event: unknown) => void
  error: (error: Error) => void
  established: (() => void) | undefined
}

type Sub = {
  id: string
  isEstablished: boolean
  isErrored: boolean
  listeners: Set<SubListener>
}

export type ClientState = 'idle' | 'connected' | 'backoff' | 'failed'
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
   // TODO: return false
  retryBehavior?: ((attempt: number) => number) | undefined

  onStateChanged?: ((newState: ClientState) => void) | undefined

  /**
   * The duration (in milliseconds) to keep the underlying WebSocket connection
   * alive when there are no active subscriptions.
   *
   * If a new subscription is started within this time, the existing connection
   * will be reused. Otherwise, once the duration has elapsed, the connection
   * will be closed.
   *
   * Set to `false` to disable idle keep-alive entirely, meaning the connection
   * will be closed immediately when the last subscription ends.
   *
   * @default 5000 (5 seconds)
   */
  idleConnectionKeepAliveTimeMs?: number | false | undefined
}

// TODO: maybe schedule all callbacks to run on the next tick, 
// so they don't cause side effects that could interfere with 
// the current flow

export type SubscribeOpts = {
  /**
   * Called when a new event is received from the subscription.
   */
  event: (event: unknown) => void
  /**
   * Called when the subscription fails.
   * This is invoked only once, since invalid subscriptions
   * are not retried.
   */
  error?: ((err: Error) => void) | undefined
  /**
   * Called when the subscription has been successfully established.
   * May be invoked multiple times, if the underlying connection is
   * re-established after a retry.
   */
  established?: () => void
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
      this.timerId = null
    }
  }
}

// 2022+ https://caniuse.com/mdn-api_crypto_randomuuid
const randomId = crypto.randomUUID?.bind(crypto) ?? (() => "10000000-1000-4000-8000-100000000000".replace(/[018]/g, c =>
  (+c ^ crypto.getRandomValues(new Uint8Array(1))[0]! & 15 >> +c / 4).toString(16)
))

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

  private readonly subsByChannelName = (client => new (class _ extends Map<string, Sub> {
    override set(key: string, value: Sub) {
      switch (client.state.type) {
        case 'connecting':
        case 'handshaking':
        case 'connected':
          clearTimeout(client.state.idleTimerId)
          client.state.idleTimerId = undefined
      }
      return super.set(key, value)
    }
    override delete(key: string) {
      const res = super.delete(key)
      if (res && this.size === 0) {
        switch (client.state.type) {
          case 'connecting':
          case 'handshaking':
          case 'connected':
            if (client.idleConnectionKeepAliveTimeMs === false) {
              client.state.closeWs()
            } else {
              client.state.idleTimerId = setTimeout(client.state.closeWs, client.idleConnectionKeepAliveTimeMs)
            }
        }
      }
      return res
    }
  })())(this)
  private readonly channelNamesById = new Map<string, string>()
  private subById = (id: string) => {
    const channel = this.channelNamesById.get(id)
    if (channel == null) {
      return null
    }
    return this.subsByChannelName.get(channel)!
  }
  private readonly pendingUnsubsBySubIds = new Map<string, Set<SubListener>>()

  private readonly retryBehavior: (attempt: number) => number
  private readonly onStateChanged: ((newState: ClientState) => void) | null
  private readonly idleConnectionKeepAliveTimeMs: number | false

  private state: {
    type: 'idle'
  } | {
    type: 'connecting'
    ws: WebSocketAdapter
    idleTimerId: ReturnType<typeof setTimeout> | undefined
    closeWs: () => void
  } | {
    type: 'handshaking'
    ws: WebSocketAdapter
    idleTimerId: ReturnType<typeof setTimeout> | undefined
    closeWs: () => void
  } | {
    type: 'connected'
    ws: WebSocketAdapter
    timeoutTimer: ResettableTimer
    idleTimerId: ReturnType<typeof setTimeout> | undefined
    closeWs: () => void
  } | {
    type: 'backoff'
    attempt: number
    timerId: ReturnType<typeof setTimeout>
  } | {
    type: 'failed'
  } = {
    type: 'idle',
  }

  constructor(endpoint: string, readonly authorization: Authorization, {
    retryBehavior = exponentialBackoffRetryBehavior(3),
    onStateChanged,
    idleConnectionKeepAliveTimeMs = 5_000,
  }: ClientOpts = {}) {
    const endpoints = parseEndpoint(endpoint)
    this.httpEndpoint = endpoints.http
    this.realtimeEndpoint = endpoints.realtime
    this.retryBehavior = retryBehavior
    this.onStateChanged = onStateChanged ?? null
    this.idleConnectionKeepAliveTimeMs = idleConnectionKeepAliveTimeMs
  }

  subscribe = (channel: string, { event, error, established }: SubscribeOpts) => {
    channel = normalizeChannel(channel)
    // TODO: dedupe by auth type
    // TODO: test what if two subscriptions are created for the same channel (in AppSync)
    let sub = this.subsByChannelName.get(channel)
    let firstSub = false
    if (sub == null) {
      firstSub = true
      this.subsByChannelName.set(channel, sub = {
        id: randomId(),
        isEstablished: false,
        isErrored: false,
        listeners: new Set(),
      })
    }

    const tryCleanupListeners = () => {
      if (sub.listeners.delete(listener) && sub.listeners.size === 0) {
        this.subsByChannelName.delete(channel)
        this.channelNamesById.delete(sub.id)
      }
    }

    const listener: SubListener = {
      event,
      error: err => {
        const pendingUnsubs = this.pendingUnsubsBySubIds.get(sub.id)
        if (pendingUnsubs?.delete(listener) && pendingUnsubs.size === 0) {
          this.pendingUnsubsBySubIds.delete(sub.id)
        }
        error?.(err)
        tryCleanupListeners()
      },
      established,
    }
    sub.listeners.add(listener)
    this.channelNamesById.set(sub.id, channel)

    switch (this.state.type) {
      // We can ignore 'connecting', 'handshaking' and 'backoff', because when
      // they are active, there is no established WebSocket connection, so 
      // the 'subscribe' message cannot be sent. When the state enter 'connected',
      // all pending subscriptions will be initiated/renewed.
      case 'idle':
      case 'failed':
        this.connect()
        break
      case 'connected':
        if (firstSub) {
          this.state.ws.send(JSON.stringify({
            type: 'subscribe',
            id: sub.id,
            channel,
            authorization: this.authorizationHeaders,
          }))
        } else if (sub.isEstablished) {
          established?.()
        }
        break
    }

    let unsubscribed = false
    const unsubscribe = () => {
      if (unsubscribed) {
        return
      }
      unsubscribed = true

      switch (this.state.type) {
        // 'idle' - should never happen: unsubscribe can be called only
        // after the first call to connect.
        //
        // If the state leaves 'connected', we have no reason to send 
        // an unsubscribe request. All subscriptions are cancelled once
        // the WebSocket is closed.
        case 'backoff':
        case 'connecting':
        case 'handshaking':
        case 'failed':
          tryCleanupListeners()
          break
        case 'connected': {
          const cleanup = () => {
            tryCleanupListeners()
            // tryCleanupListeners has side-effect, so the state could be changed
            if (this.state.type === 'connected' && sub.listeners.size === 0) {
              this.state.ws.send(JSON.stringify({ type: 'unsubscribe', id: sub.id }))
            }
          }
          if (sub.isEstablished || sub.isErrored) {
            cleanup()
            break
          }

          let pendingUnsubs: Set<SubListener> | undefined
          ;(pendingUnsubs = this.pendingUnsubsBySubIds.get(sub.id)) ?? this.pendingUnsubsBySubIds.set(sub.id, pendingUnsubs = new Set())
          pendingUnsubs.add(listener)

          // We don't handle subscribe_error. In that case, we don't need to
          // explicitly unsubscribe.
          //
          // If unsubscribe is sent at the same time as subscribe, it will be
          // silently ignored. This is why we wait for the sub to establish
          // before unsubscribing.
          listener.established = () => {
            if (pendingUnsubs.delete(listener) && pendingUnsubs.size === 0) {
              this.pendingUnsubsBySubIds.delete(sub.id)
            }
            cleanup()
          }

          break
        }
      }
    }

    return {
      unsubscribe,
    }
  }

  connect = () => {
    const dontInitConnection = this.idleConnectionKeepAliveTimeMs === false && this.subsByChannelName.size === 0
    let attempt = 0
    switch (this.state.type) {
      case 'backoff':
        attempt = this.state.attempt
        clearTimeout(this.state.timerId)
      case 'failed':
        if (dontInitConnection) {
          this.state = { type: 'idle' }
          this.onStateChanged?.('idle')
          return
        }
      case 'idle':
        if (dontInitConnection) {
          return
        }
        break
      case 'connecting':
      case 'handshaking':
      case 'connected':
        return
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

    const closeWs = () => {
      ws.close()
      connectionEnded(true)
    }

    this.state = {
      type: 'connecting',
      ws,
      closeWs,
      idleTimerId: undefined,
    }

    const onOpen = () => {
      ws.removeEventListener('open', onOpen)
      // 'open' cannot be called after 'error' or 'close'; current state MUST
      // be 'connecting'
      const state = this.state as (typeof this.state & { type: 'connecting' })
      this.state = {
        type: 'handshaking',
        ws,
        closeWs,
        idleTimerId: state.idleTimerId
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
        // 'idle' - should never happen: the ws is created only after
        // entering the 'connecting' state
        //
        // 'connecting' - should never happen: message cannot be delivered
        // before the 'open' state
        //
        // 'backoff', 'failed' - should never happen: the 'message' listener is
        // removed before leaving the 'connected' state
        case 'handshaking':
          // TODO: ack timeout
          if (message.type !== 'connection_ack') {
            throw new Error(`[aws-appsync-events bug] handshake error: expected "connection_ack" but got ${JSON.stringify(message.type)}`)
          }
          this.state = {
            type: 'connected',
            ws,
            timeoutTimer: new ResettableTimer(message.connectionTimeoutMs, () => {
              ws.close()
              connectionEnded(false)
            }),
            idleTimerId: this.state.idleTimerId,
            closeWs,
          }
          if (this.subsByChannelName.size === 0) {
            // - if connect was called explicitly and idleConnectionKeepAliveTimeMs is false,
            // connect early returns
            // - if connect was called implicitly (via subscribe), idleConnectionKeepAliveTimeMs === false
            // situation is already handled (because the current state is 'handshaking')
            if (this.idleConnectionKeepAliveTimeMs === false) {
              throw new Error('[aws-appsync-events bug] expected idle connection keep alive to be enabled')
            }
            clearTimeout(this.state.idleTimerId)
            this.state.idleTimerId = setTimeout(closeWs, this.idleConnectionKeepAliveTimeMs as number)
          }
          for (const [channel, sub] of this.subsByChannelName) {
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
              sub.isEstablished = true
              // copy listeners (established() can add new listeners)
              for (const listener of Array.from(sub.listeners)) {
                listener.established?.()
              }
              break
            }
            case 'subscribe_error': {
              const sub = this.subById(message.id)
              if (sub == null) {
                throw new Error(`[aws-appsync-events bug] subscribe_error for unknown sub`)
              }
              const error = new Error('Subscribe error' + normalizeErrors(message.errors))
              sub.isErrored = true
              // error() can add new listeners; they're always appended at the end,
              // and are iterated here as well
              for (const listener of sub.listeners) {
                listener.error(error)
              }
              break
            }
            case 'data': {
              const channel = this.channelNamesById.get(message.id)
              if (channel == null) {
                break
              }
              const event = JSON.parse(message.event)
              for (const listener of this.subsByChannelName.get(channel)!.listeners) {
                listener.event?.(event)
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

    const connectionEnded = (graceful: boolean) => {
      removeEventListeners()

      switch (this.state.type) {
        // 'idle', 'backoff', 'failed' - there is no chance for connectionEnded to be
        // called while the client is in any of these states
        case 'connected':
          this.state.timeoutTimer.cancel()
        case 'connecting':
        case 'handshaking': {
          clearTimeout(this.state.idleTimerId)
          for (const [subId, listeners] of this.pendingUnsubsBySubIds) {
            const channel = this.channelNamesById.get(subId)!
            const sub = this.subsByChannelName.get(channel)!
            for (const listener of listeners) {
              sub.listeners.delete(listener)
            }
            if (sub.listeners.size === 0) {
              this.subsByChannelName.delete(channel)
              this.channelNamesById.delete(subId)
            }
            listeners.clear()
          }
          for (const sub of this.subsByChannelName.values()) {
            sub.isEstablished = false
            sub.isErrored = false
          }
          this.pendingUnsubsBySubIds.clear()
          if (graceful) {
            if (this.state.type === 'connected') {
              this.onStateChanged?.('idle')
            }
            this.state = { type: 'idle' }
            break
          }
          const newAttempt = attempt + 1
          const msToSleep = this.retryBehavior(newAttempt)
          if (msToSleep < 0) {
            this.state = { type: 'failed' }
            this.onStateChanged?.('failed')
            break
          }
          this.state = {
            type: 'backoff',
            attempt: newAttempt,
            timerId: setTimeout(this.connect, msToSleep),
          }
          this.onStateChanged?.('backoff')
          break
        }
      }
    }

    const onClose = () => connectionEnded(false)

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

  close = () => {
    switch (this.state.type) {
      // 'failed' - leave it as is
      case 'connecting':
      case 'handshaking':
      case 'connected':
        this.state.closeWs()
        break
      case 'backoff':
        clearTimeout(this.state.timerId)
        this.state = { type: 'idle' }
        this.onStateChanged?.('idle')
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
