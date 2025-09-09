import { sigV4 } from './sigv4.js'

const provisionedEndpointRe =
  /^(\w{26}\.(appsync|appsync-realtime)-api\.\w{2}(?:(?:-\w{2,})+)-\d\.amazonaws.com(?:\.cn)?)(?:\/event(?:\/realtime)?)?$/i
const protocolRe = /^(?:(?:(?:https?)|(?:wss?)):\/\/)?/i
const pathnameRe = /(?:\/(?:event(?:\/(?:realtime\/?)?)?)?)?$/i

export function parseEndpoint(endpoint: string): {
  http: string
  realtime: string
} {
  const domainName = endpoint.replace(protocolRe, '').replace(pathnameRe, '')

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

function normalizeErrors(errors: unknown) {
  return Array.isArray(errors)
    ? `: ` +
        errors
          .map(
            (e: any) =>
              String(e?.errorType) +
              (e?.errorCode != null ? ` ${String(e.errorCode)}` : '') +
              (e?.message != null ? ` (${String(e.message)})` : ''),
          )
          .join(', ')
    : ''
}

export type AuthorizerOpts = {
  httpEndpoint: string
  realtimeEndpoint: string
  message:
    | {
        type: 'connect'
      }
    | {
        type: 'subscribe'
        channel: string
      }
    | {
        type: 'publish'
        channel: string
        events: unknown[]
      }
}

export type Authorizer = (
  opts: AuthorizerOpts,
) => Promise<Record<string, string>> | Record<string, string>

export function apiKeyAuthorizer(apiKey: string): Authorizer {
  return opts => ({
    host: opts.httpEndpoint,
    'x-api-key': apiKey,
  })
}

export function cognitoUserPoolsAuthorizer(jwtIdToken: string): Authorizer {
  return opts => ({
    host: opts.httpEndpoint,
    Authorization: jwtIdToken,
  })
}

export function openIdConnectAuthorizer(jwtIdToken: string): Authorizer {
  return opts => ({
    host: opts.httpEndpoint,
    Authorization: jwtIdToken,
  })
}

export function lambdaAuthorizer(authorizationToken: string): Authorizer {
  return opts => ({
    host: opts.httpEndpoint,
    Authorization: authorizationToken,
  })
}

export function awsIamAuthorizer(config: {
  accessKeyId: string
  secretAccessKey: string
  region: string
  sessionToken?: string | undefined
}): Authorizer {
  return async opts => {
    const headers = {
      accept: 'application/json, text/javascript',
      'content-encoding': 'amz-1.0',
      'content-type': 'application/json; charset=UTF-8',
      host: opts.httpEndpoint,
    }
    const { xAmzDate, authorization } = await sigV4({
      region: config.region,
      service: 'appsync',
      method: 'POST',
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
      headers,
      url: `https://${opts.httpEndpoint}/event`,
      body: JSON.stringify(
        (() => {
          switch (opts.message.type) {
            case 'connect':
              return {}
            case 'subscribe':
              return { channel: opts.message.channel }
            case 'publish':
              return {
                channel: opts.message.channel,
                events: opts.message.events,
              }
          }
        })(),
      ),
    })
    return {
      ...headers,
      ...(config.sessionToken != null && {
        'X-Amz-Security-Token': config.sessionToken,
      }),
      'x-amz-date': xAmzDate,
      Authorization: authorization,
    }
  }
}

type Sub = {
  id: string
  isEstablished: boolean
  channel: string
  event: (event: unknown) => void
  error: (error: Error) => void
  established: (() => void) | undefined
  authorizer: Authorizer
}

export type ClientState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'backoff'
  | 'failed'
export type ClientOpts = {
  /**
   * Determines the delay between retry attempts.
   *
   * Defaults to **exponential backoff with jitter** (see
   * {@link https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/ | AWS blog}),
   * up to **3 attempts**.
   *
   * @param attempt The current retry attempt number (starting from 1).
   * @returns The delay in milliseconds before the next retry attempt,
   * or `false` to stop retrying and treat the connection as failed.
   */
  retryBehavior?: ((attempt: number) => number | false) | undefined

  /**
   * Callback invoked whenever the client's connection state changes.
   *
   * @param newState The new state of the client after the transition.
   */
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
   * @default 5000
   */
  idleConnectionKeepAliveTimeMs?: number | false | undefined

  /**
   * The default authorizer for `subscribe` requests.
   * If no specific authorizer is provided for a `subscribe()` call, this default authorizer will be used.
   * By default, this is set to client's `connectionAuthorizer`.
   */
  defaultSubscribeAuthorizer?: Authorizer | undefined

  /**
   * The default authorizer for `publish` requests.
   * If no specific authorizer is provided for a `publish()` call, this default authorizer will be used.
   * By default, this is set to client's `connectionAuthorizer`.
   */
  defaultPublishAuthorizer?: Authorizer | undefined
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
  established?: (() => void) | undefined

  /**
   * The authorizer to use when subscribing.
   *
   * By default, client's `defaultSubscribeAuthorizer` will be used.
   */
  authorizer?: Authorizer | undefined
}

export type PublishOpts = {
  /**
   * The authorizer to use when publishing.
   *
   * By default, client's `defaultPublishAuthorizer` will be used.
   */
  authorizer?: Authorizer | undefined
}

// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
export function exponentialBackoffRetryBehavior(
  maxAttempts: number,
): (attempt: number) => number | false {
  return attempt =>
    attempt <= maxAttempts && Math.min(Math.random() * 2 ** attempt, 20) * 1_000
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
const randomId =
  crypto.randomUUID?.bind(crypto) ??
  (() =>
    // https://stackoverflow.com/a/2117523
    '10000000-1000-4000-8000-100000000000'.replace(/[018]/g, c =>
      (
        +c ^
        (crypto.getRandomValues(new Uint8Array(1))[0]! & (15 >> (+c / 4)))
      ).toString(16),
    ))

export type WebSocketAdapter = {
  addEventListener: <K extends keyof WebSocketEventMap>(
    type: K,
    listener: (ev: WebSocketEventMap[K]) => any,
  ) => void
  removeEventListener: <K extends keyof WebSocketEventMap>(
    type: K,
    listener: (ev: WebSocketEventMap[K]) => any,
  ) => void
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

  private readonly subsById = (client =>
    new (class _ extends Map<string, Sub> {
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
                client.state.idleTimerId = setTimeout(
                  client.state.closeWs,
                  client.idleConnectionKeepAliveTimeMs,
                )
              }
          }
        }
        return res
      }
    })())(this)

  private readonly pendingUnsubIds = new Set<string>()

  private readonly retryBehavior: (attempt: number) => number | false
  private readonly onStateChanged: ((newState: ClientState) => void) | null
  private readonly idleConnectionKeepAliveTimeMs: number | false

  private state:
    | {
        type: 'idle'
      }
    | {
        type: 'auth-preparing'
      }
    | {
        type: 'connecting'
        ws: WebSocketAdapter
        idleTimerId: ReturnType<typeof setTimeout> | undefined
        closeWs: () => void
      }
    | {
        type: 'handshaking'
        ws: WebSocketAdapter
        timerId: ReturnType<typeof setTimeout>
        idleTimerId: ReturnType<typeof setTimeout> | undefined
        closeWs: () => void
      }
    | {
        type: 'connected'
        ws: WebSocketAdapter
        timeoutTimer: ResettableTimer
        idleTimerId: ReturnType<typeof setTimeout> | undefined
        closeWs: () => void
      }
    | {
        type: 'backoff'
        attempt: number
        timerId: ReturnType<typeof setTimeout>
      }
    | {
        type: 'failed'
      } = {
    type: 'idle',
  }

  private readonly defaultSubscribeAuthorizer: Authorizer
  private readonly defaultPublishAuthorizer: Authorizer

  constructor(
    endpoint: string,
    readonly connectionAuthorizer: Authorizer,
    {
      retryBehavior = exponentialBackoffRetryBehavior(3),
      onStateChanged,
      idleConnectionKeepAliveTimeMs = 5_000,
      defaultSubscribeAuthorizer = connectionAuthorizer,
      defaultPublishAuthorizer = connectionAuthorizer,
    }: ClientOpts | undefined = {},
  ) {
    const endpoints = parseEndpoint(endpoint)
    this.httpEndpoint = endpoints.http
    this.realtimeEndpoint = endpoints.realtime
    this.retryBehavior = retryBehavior
    this.onStateChanged =
      onStateChanged != null
        ? s => queueMicrotask(() => onStateChanged(s))
        : null
    this.idleConnectionKeepAliveTimeMs = idleConnectionKeepAliveTimeMs
    this.defaultSubscribeAuthorizer = defaultSubscribeAuthorizer
    this.defaultPublishAuthorizer = defaultPublishAuthorizer
  }

  /**
   * Start a realtime subscription on a channel
   */
  subscribe = (channel: string, options: SubscribeOpts) => {
    const { event, error, established, authorizer } = options
    const id = randomId()

    const callEstablished = established && (() => queueMicrotask(established))
    const callError =
      error && ((err: Error) => queueMicrotask(() => error(err)))
    const cleanupSub = () => {
      this.pendingUnsubIds.delete(id)
      this.subsById.delete(id)
    }

    const sub: Sub = {
      id,
      isEstablished: false,
      channel,
      event: e => queueMicrotask(() => event(e)),
      error: err => {
        callError?.(err)
        cleanupSub()
      },
      established: callEstablished,
      authorizer: authorizer ?? this.defaultSubscribeAuthorizer,
    }
    this.subsById.set(id, sub)

    switch (this.state.type) {
      // We can ignore 'auth-preparing', 'connecting', 'handshaking' and 'backoff',
      // because when they are active, there is no established WebSocket connection,
      // so the 'subscribe' message cannot be sent. When the state enter 'connected',
      // all pending subscriptions will be initiated/renewed.
      case 'idle':
      case 'failed':
        this.connect()
        break
      case 'connected':
        this.wsSubscribe(this.state.ws, id, channel, sub.authorizer)
    }

    let unsubscribed = false

    return {
      unsubscribe: (): void => {
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
          case 'auth-preparing':
          case 'connecting':
          case 'handshaking':
          case 'failed':
            cleanupSub()
            break
          case 'connected': {
            this.pendingUnsubIds.add(id)

            const cleanup = () => {
              cleanupSub()
              // cleanupSub has side-effect, so the state could be changed
              if (this.state.type === 'connected') {
                this.state.ws.send(
                  JSON.stringify({ type: 'unsubscribe', id: id }),
                )
              }
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
            sub.established = cleanup
          }
        }
      },
    }
  }

  connect: () => void = async () => {
    const dontInitConnection =
      this.idleConnectionKeepAliveTimeMs === false && this.subsById.size === 0
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
      case 'auth-preparing':
      case 'connecting':
      case 'handshaking':
      case 'connected':
        return
    }

    this.state = { type: 'auth-preparing' }
    this.onStateChanged?.('connecting')

    const authPayload = btoa(
      JSON.stringify(
        await this.authorizationHeaders(
          { type: 'connect' },
          this.connectionAuthorizer,
        ),
      ),
    )
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/g, '')

    // If two connects are auth-preparing simultaneously, the first one skips this
    // condition and synchronously changes the state to 'connecting' below. The
    // second connect then cannot skip this condition and abandons its connection
    // attempt.
    if (this.state.type !== 'auth-preparing') {
      return
    }

    const ws = new this.wsCtor(
      `wss://${this.realtimeEndpoint}/event/realtime`,
      ['aws-appsync-event-ws', `header-${authPayload}`],
    )

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
      const state = this.state as typeof this.state & { type: 'connecting' }
      this.state = {
        type: 'handshaking',
        ws,
        closeWs,
        timerId: setTimeout(() => {
          ws.close()
          connectionEnded(false)
        }, 15_000),
        idleTimerId: state.idleTimerId,
      }
      ws.send(JSON.stringify({ type: 'connection_init' }))
    }
    const onMessage = (event: MessageEvent) => {
      if (typeof event.data !== 'string') {
        throw new Error(
          `[aws-appsync-events bug] unexpected binary data in message: ${event.data}`,
        )
      }

      const message = JSON.parse(event.data)

      switch (this.state.type) {
        // 'idle', 'auth-preparing' - should never happen: the ws is created
        // only after entering the 'connecting' state
        //
        // 'connecting' - should never happen: message cannot be delivered
        // before the 'open' state
        //
        // 'backoff', 'failed' - should never happen: the 'message' listener is
        // removed before leaving the 'connected' state
        case 'handshaking':
          clearTimeout(this.state.timerId)
          switch (message.type) {
            case 'connection_ack':
              if (
                typeof message.connectionTimeoutMs !== 'number' ||
                message.connectionTimeoutMs <= 0
              ) {
                this.fail()
                throw new Error(
                  `[aws-appsync-events bug] expected "connection_ack" message to have positive numeric "connectionTimeoutMs" (message: ${JSON.stringify(message)})`,
                )
              }
              this.state = {
                type: 'connected',
                ws,
                timeoutTimer: new ResettableTimer(
                  message.connectionTimeoutMs,
                  () => {
                    ws.close()
                    connectionEnded(false)
                  },
                ),
                idleTimerId: this.state.idleTimerId,
                closeWs,
              }
              if (this.subsById.size === 0) {
                // - if connect was called explicitly and idleConnectionKeepAliveTimeMs is false,
                // connect early returns
                // - if connect was called implicitly (via subscribe), idleConnectionKeepAliveTimeMs === false
                // situation is already handled (because the current state is 'handshaking')
                if (this.idleConnectionKeepAliveTimeMs === false) {
                  this.fail()
                  throw new Error(
                    '[aws-appsync-events bug] expected idle connection keep alive to be enabled',
                  )
                }
                clearTimeout(this.state.idleTimerId)
                this.state.idleTimerId = setTimeout(
                  closeWs,
                  this.idleConnectionKeepAliveTimeMs as number,
                )
              }
              for (const sub of this.subsById.values()) {
                this.wsSubscribe(
                  this.state.ws,
                  sub.id,
                  sub.channel,
                  sub.authorizer,
                )
              }
              this.onStateChanged?.('connected')
              break
            case 'connection_error':
              this.fail()
              throw new Error(
                '[aws-appsync-events] connection error' +
                  normalizeErrors(message.errors),
              )
            default:
              this.fail()
              throw new Error(
                `[aws-appsync-events bug] handshake error: expected "connection_ack" message but got ${JSON.stringify(message)}`,
              )
          }
          break
        case 'connected':
          switch (message.type) {
            case 'ka':
              this.state.timeoutTimer.reset()
              break
            case 'subscribe_success': {
              if (typeof message.id !== 'string') {
                throw new Error(
                  `[aws-appsync-events bug] expected "subscribe_success" message to have "id" (message: ${JSON.stringify(message)})`,
                )
              }
              const sub = this.subsById.get(message.id)
              if (sub == null) {
                throw new Error(
                  `[aws-appsync-events bug] subscribe_success for unknown sub`,
                )
              }
              sub.isEstablished = true
              sub.established?.()
              break
            }
            case 'subscribe_error': {
              if (typeof message.id !== 'string') {
                throw new Error(
                  `[aws-appsync-events bug] expected "subscribe_error" message to have "id" (message: ${JSON.stringify(message)})`,
                )
              }
              const sub = this.subsById.get(message.id)
              if (sub == null) {
                throw new Error(
                  `[aws-appsync-events bug] subscribe_error for unknown sub`,
                )
              }
              sub.error(
                new Error('Subscribe error' + normalizeErrors(message.errors)),
              )
              break
            }
            case 'data':
              if (typeof message.id !== 'string') {
                throw new Error(
                  `[aws-appsync-events bug] expected "data" message to have "id" (message: ${JSON.stringify(message)})`,
                )
              }
              if (typeof message.event !== 'string') {
                throw new Error(
                  `[aws-appsync-events bug] expected "data" message to have "event" (message: ${JSON.stringify(message)})`,
                )
              }
              this.subsById.get(message.id)?.event(JSON.parse(message.event))
              break
            case 'unsubscribe_success':
              break
            case 'unsubscribe_error':
              throw new Error(
                '[aws-appsync-events bug] unsubscribe error' +
                  normalizeErrors(message.errors),
              )
            case 'error':
              throw new Error(
                '[aws-appsync-events] unknown error' +
                  normalizeErrors(message.errors),
              )
            default:
              throw new Error(
                `[aws-appsync-events] unknown message: ${JSON.stringify(message)}`,
              )
          }
      }
    }

    const connectionEnded = (graceful: boolean) => {
      removeEventListeners()

      switch (this.state.type) {
        // 'idle', 'auth-preparing', 'backoff', 'failed' - there is no chance for
        // connectionEnded to be called while the client is in any of these states
        case 'connected':
          this.state.timeoutTimer.cancel()
        case 'connecting':
        case 'handshaking': {
          clearTimeout(this.state.idleTimerId)
          if (this.state.type === 'handshaking') {
            clearTimeout(this.state.timerId)
          }
          for (const subId of this.pendingUnsubIds) {
            this.subsById.delete(subId)
          }
          for (const sub of this.subsById.values()) {
            sub.isEstablished = false
          }
          this.pendingUnsubIds.clear()
          if (graceful) {
            this.state = { type: 'idle' }
            this.onStateChanged?.('idle')
            break
          }
          const newAttempt = attempt + 1
          const msToSleep = this.retryBehavior(newAttempt)
          if (msToSleep === false) {
            this.fail()
            break
          }
          this.onStateChanged?.('backoff')
          this.state = {
            type: 'backoff',
            attempt: newAttempt,
            timerId: setTimeout(this.connect, msToSleep),
          }
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

  close = (): void => {
    switch (this.state.type) {
      // 'idle', 'failed' - leave it as is
      case 'backoff':
        clearTimeout(this.state.timerId)
      case 'auth-preparing':
        this.state = { type: 'idle' }
        this.onStateChanged?.('idle')
        break
      case 'connecting':
      case 'handshaking':
      case 'connected':
        this.state.closeWs()
    }
  }

  /**
   * Publishes a list of events to the specified channel.
   *
   * @param channel The channel name where the events will be published.
   * @param events An array of events to publish.
   *   - Up to **5 events** can be processed per request.
   *   - If more than 5 events are provided, they are automatically split into batches of up to 5 events each.
   * @param options - Optional {@link PublishOpts | `PublishOpts`}
   * @returns A promise that resolves when all event batches have been published.
   */
  publishHttp = async (
    channel: string,
    events: unknown[],
    options: PublishOpts | undefined = {},
  ): Promise<void> => {
    if (events.length === 0) {
      return
    }
    let { authorizer } = options
    if (authorizer == null) {
      authorizer = this.defaultPublishAuthorizer
    }
    const batches: string[][] = [[]]
    for (const event of events) {
      let batch = batches[batches.length - 1]!
      // AppSync allows batching up to 5 events
      // https://docs.aws.amazon.com/appsync/latest/eventapi/publish-http.html
      if (batch.length === 5) {
        batches.push((batch = []))
      }
      batch.push(JSON.stringify(event))
    }

    await Promise.all(
      batches.map(async batch => {
        let attempt = 0
        let response: Response
        while (true) {
          try {
            const { host: _, ...authHeaders } = await this.authorizationHeaders(
              { type: 'publish', channel, events: batch },
              authorizer,
            )
            response = await fetch(`https://${this.httpEndpoint}/event`, {
              method: 'POST',
              headers: {
                'content-type': 'application/json',
                ...authHeaders,
              },
              body: JSON.stringify({ channel, events: batch }),
            })
            break
          } catch (e) {
            const waitMs = this.retryBehavior(++attempt)
            if (waitMs === false) {
              throw e
            }
            await new Promise(resolve => setTimeout(resolve, waitMs))
          }
        }
        const responseBodyText = await response.text()
        let res: unknown
        try {
          res = JSON.parse(responseBodyText)
        } catch {
          throw new Error(
            `Publish error: ${response.statusText} ${response.status}${responseBodyText}`,
          )
        }
        if (typeof res !== 'object' || res == null) {
          return
        }
        if ('errors' in res) {
          throw new Error(`Publish error${normalizeErrors(res.errors)}`)
        }
        if (
          'failed' in res &&
          Array.isArray(res.failed) &&
          res.failed.length > 0
        ) {
          throw new Error(
            `Publish errors: ${JSON.stringify(res.failed, null, 2)}`,
          )
        }
      }),
    )
  }

  // TODO
  // private publishWebSocket

  private authorizationHeaders = async (
    message: AuthorizerOpts['message'],
    authorizer: Authorizer,
  ) => {
    return await authorizer({
      httpEndpoint: this.httpEndpoint,
      realtimeEndpoint: this.realtimeEndpoint,
      message,
    })
  }

  private wsSubscribe = async (
    ws: WebSocketAdapter,
    subId: string,
    channel: string,
    authorizer: Authorizer,
  ) =>
    ws.send(
      JSON.stringify({
        type: 'subscribe',
        id: subId,
        channel,
        authorization: await this.authorizationHeaders(
          {
            type: 'subscribe',
            channel,
          },
          authorizer,
        ),
      }),
    )

  private fail = () => {
    this.state = { type: 'failed' }
    this.onStateChanged?.('failed')
  }
}
