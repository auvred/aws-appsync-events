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
  error: ((error: Error) => void) | undefined
  established: (() => void) | undefined
}

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

  private readonly subsByChannelName = new Map<string, {
    id: string
    isEstablished: boolean
    listeners: Set<SubListener>
  }>()
  private readonly channelNamesById= new Map<string, string>()
  private subById = (id: string) => {
    const channel = this.channelNamesById.get(id)
    if (channel == null) {
      return null
    }
    return this.subsByChannelName.get(channel)!
  }
  private readonly pendingUnsubIds = new Set<string>()

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

  subscribe = (channel: string, { event, error, established }: SubscribeOpts) => {
    channel = normalizeChannel(channel)
    // TODO: dedupe by auth type
    // TODO: test what if two subscriptions are created for the same channel
    let sub = this.subsByChannelName.get(channel)
    let firstSub = false
    if (sub == null) {
      firstSub = true
      this.subsByChannelName.set(channel, sub = {
        // TODO: polyfill for older enrironments
        // https://caniuse.com/mdn-api_crypto_randomuuid
        id: crypto.randomUUID(),
        isEstablished: false,
        listeners: new Set(),
      })
    }

    const tryCleanupListeners = () => {
      if (sub.listeners.size === 1) {
        // TODO: should we close connection?
        this.subsByChannelName.delete(channel)
        this.channelNamesById.delete(sub.id)
      } else {
        sub.listeners.delete(listener)
      }
    }

    const listener: SubListener = {
      event,
      error: err => {
        tryCleanupListeners()
        error?.(err)
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
        } else {
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
          tryCleanupListeners()
          break
        case 'connected': {
          const cleanup = () => {
            tryCleanupListeners()
            // It's safe to use type assertion here because listener.established
            // is always called synchronously by the 'subscribe_success' handler,
            // which can only be invoked when the state is 'connected'.
            ;(this.state as typeof this.state & { type: 'connected' }).ws.send(JSON.stringify({ type: 'unsubscribe', id: sub.id }))
          }
          if (sub.isEstablished) {
            cleanup()
            break
          }
          // We don't handle subscribe_error. In that case, we don't need to
          // explicitly unsubscribe.
          //
          // If unsubscribe is sent at the same time as subscribe, it will be
          // silently ignored. This is why we wait for the sub to establish
          // before unsubscribing.
          listener.established = () => {
            this.pendingUnsubIds.delete(sub.id)
            cleanup()
            established?.()
          }
          this.pendingUnsubIds.add(sub.id)
          break
        }
        case 'failed': // TODO
          break
      }
    }

    return {
      unsubscribe: () => {
        unsubscribe()
      },
    }
  }

  private connect = () => {
    // TODO: should we still open connection if there is no active subs
    let attempt = 0
    switch (this.state.type) {
      // 'idle' - connect
      // 'failed' - reconnect
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
      // 'open' cannot be called after 'error' or 'close'; current state MUST
      // be 'connecting'
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
        // 'idle' - should never happen: the ws is created only after
        // entering the 'connecting' state
        //
        // 'connecting' - should never happen: message cannot be delivered
        // before the 'open' state
        //
        // 'backoff', 'failed' - should never happen: the 'message' listener is
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
              for (const listener of sub.listeners) {
                // !!!
                listener.established?.()
              }
              break
            }
            // TODO: do not resub for errored subs
            case 'subscribe_error': {
              const sub = this.subById(message.id)
              if (sub == null) {
                throw new Error(`[aws-appsync-events bug] subscribe_error for unknown sub`)
              }
              const error = new Error('Subscribe error' + normalizeErrors(message.errors))
              for (const listener of sub.listeners) {
                // !!!
                listener.error?.(error)
              }
              break
            }
            case 'data': {
              const channel = this.channelNamesById.get(message.id)
              if (channel == null) {
                break
              }
              for (const listener of this.subsByChannelName.get(channel)!.listeners) {
                listener.event?.(JSON.parse(message.event))
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
      // TODO: handle manual close
      removeEventListeners()

      switch (this.state.type) {
        case 'connected':
          this.state.timeoutTimer.cancel()
        case 'idle':
        case 'connecting':
        case 'handshaking': {
          for (const subId of this.pendingUnsubIds) {
            const channel = this.channelNamesById.get(subId)!
            this.subsByChannelName.delete(channel)
            this.channelNamesById.delete(subId)
          }
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
