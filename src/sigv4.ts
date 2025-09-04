// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html

let encoder: TextEncoder | null = null

const arrayBufferHex: (buf: ArrayBuffer) => string = 'toHex' in Uint8Array.prototype
  // @ts-expect-error https://github.com/tc39/proposal-arraybuffer-base64/issues/51
  ? buf => new Uint8Array(buf).toHex()
  : buf => {
    let res = ''
    for (const n of new Uint8Array(buf)) {
      res += ((n >> 4) & 0xf).toString(16) + (n & 0xf).toString(16)
    }
    return res
  }

async function sha256hex(data: string) {
  return arrayBufferHex(await crypto.subtle.digest('SHA-256', (encoder ??= new TextEncoder()).encode(data)))
}

async function hmac(key: string | ArrayBuffer, data: string) {
  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    typeof key === 'string' ? (encoder ??= new TextEncoder()).encode(key) : key,
    { name: 'HMAC', hash: { name: 'SHA-256' } },
    false,
    ['sign'],
  )
  return await crypto.subtle.sign('HMAC', cryptoKey, (encoder ??= new TextEncoder()).encode(data))
}

// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/encodeURIComponent#encoding_for_rfc3986
function encodeRFC3986(str: string) {
  return str.replace(
    /[!'()*]/g,
    (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}

export type SigV4Opts = {
  headers: Record<string, string>
  method: string
  body?: string | undefined
  signingDate?: Date | undefined
  region: string
  service: string
  accessKeyId: string
  secretAccessKey: string
  url: string
}
export async function sigV4(opts: SigV4Opts) {
  const headers = Object.keys(opts.headers)
    .map(k => [k.toLowerCase(), opts.headers[k]!.replace(/^s+|s+$/g, '').replace(/\s+/g, ' ')] as const)
    .sort(([a], [b]) => a.localeCompare(b))

  const signedHeaders = headers.map(([k]) => k).join(';')

  const datetime = (opts.signingDate ?? new Date()).toISOString().replace(/[:-]|\.\d{3}/g, '')
  const date = datetime.slice(0, 8)

  const url = new URL(opts.url)

  const canonicalRequest = [
    opts.method,
    url.pathname,
    [...url.searchParams]
      .map(([k, v]) => [encodeRFC3986(encodeURIComponent(k)), encodeRFC3986(encodeURIComponent(v))] as const)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(p => p.join('='))
      .join('&'),
    headers.map(([k, v]) => `${k}:${v}\n`).join(''),
    signedHeaders,
    await sha256hex(opts.body ?? ''),
  ].join('\n')
  
  const credentialScope = [
    date,
    opts.region,
    opts.service,
    'aws4_request'
  ].join('/')

  const toSign = [
    'AWS4-HMAC-SHA256',
    datetime,
    credentialScope,
    await sha256hex(canonicalRequest),
  ].join('\n')

  const signingKey = await hmac(await hmac(await hmac(await hmac(`AWS4${opts.secretAccessKey}`, date), opts.region), opts.service), 'aws4_request')
  const signature = arrayBufferHex(await hmac(signingKey, toSign))
  return {
    xAmzDate: datetime,
    authorization: `AWS4-HMAC-SHA256 Credential=${opts.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`
  }
}
