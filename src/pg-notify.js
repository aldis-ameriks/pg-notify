import EventEmitter from 'node:events'
import util from 'node:util'
import pg from 'pg'
import format from 'pg-format'
import sjson from 'secure-json-parse'

const sleep = util.promisify(setTimeout)

const states = {
  init: 'init',
  closing: 'closing',
  reconnecting: 'reconnecting',
  connected: 'connected'
}

class PGPubSub {
  constructor (opts) {
    this.opts = opts
    this.ee = new EventEmitter()
    this.ee.setMaxListeners(0)

    this.reconnectMaxRetries = typeof opts.reconnectMaxRetries !== 'undefined' ? opts.reconnectMaxRetries : 10
    this.maxPayloadSize = typeof opts.maxPayloadSize !== 'undefined' ? opts.maxPayloadSize : 7999 // default on a standard pg installation
    if (opts.onConnectionError && typeof opts.onConnectionError !== 'function') {
      throw new TypeError('onConnectionError must be a function')
    }
    this.onConnectionError = opts.onConnectionError || null

    this.state = states.init
    this.reconnectRetries = 0
    this.channels = {}
    /* c8 ignore next */
    this.debug = process.env.DEBUG && process.env.DEBUG.includes('pg-notify')
  }

  async emit (channel, payload) {
    if (this.state !== states.connected) {
      throw new Error('[PGPubSub]: not connected')
    }

    this._debug('[emit] channel: ', channel)
    this._debug('[emit] payload: ', payload)
    this._debug('[emit] state: ', this.state)

    if (payload == null) {
      return this.client.query(`NOTIFY ${format.ident(channel)}`)
    }

    if (typeof payload === 'object') {
      payload = JSON.stringify(payload)
    }

    const parsedPayload = format.literal(payload)

    if (Buffer.byteLength(parsedPayload, 'utf-8') > this.maxPayloadSize) {
      throw new Error(`[PGPubSub]: payload exceeds maximum size: ${this.maxPayloadSize}`)
    }

    return this.client.query(`NOTIFY ${format.ident(channel)}, ${parsedPayload}`)
  }

  async on (channel, listener) {
    this._debug('[subscribe]', channel)
    if (this.state !== states.connected) {
      throw new Error('[PGPubSub]: not connected')
    }

    if (this.channels[channel]) {
      this.ee.on(channel, listener)
      this.channels[channel].listeners++
      return
    }

    await this.client.query(`LISTEN ${format.ident(channel)}`)

    this.ee.on(channel, listener)
    this.channels[channel] = { listeners: 1 }
  }

  async off (channel, listener) {
    return this.removeListener(channel, listener)
  }

  async removeListener (channel, listener) {
    if (!this.channels[channel]) {
      return
    }

    this.ee.removeListener(channel, listener)
    this.channels[channel].listeners--

    if (this.channels[channel].listeners === 0) {
      delete this.channels[channel]
      if (this.state === states.connected) {
        return this.client.query(`UNLISTEN ${format.ident(channel)}`)
      }
    }
  }

  async connect () {
    this.reconnectRetries = 0

    if (this.client) {
      try { await this.client.end() } catch {}
    }

    this.state = states.init

    try {
      await this._setupClient()
    } catch (e) {
      await this._reconnect()
    }
  }

  async close () {
    if (this.state === states.closing) {
      return
    }
    this.state = states.closing
    this.channels = {}
    this.ee.removeAllListeners()
    if (this.client) {
      await this.client.end()
    }
  }

  async _reconnect (force) {
    this._debug('[_reconnect] state: ', this.state)

    if (!this.reconnectMaxRetries) {
      await this.close()
      return
    }

    if ([states.reconnecting, states.closing].includes(this.state) && !force) {
      return
    }

    this.state = states.reconnecting
    this.reconnectRetries++

    try {
      await this.client.end()
      await this._setupClient()
    } catch (err) {
      if (this.reconnectRetries >= this.reconnectMaxRetries) {
        await this.close()
        throw new Error('[PGPubSub]: max reconnect attempts reached, aborting', { cause: err })
      }
      if (![states.closing, states.connected].includes((this.state))) {
        await sleep(10)
        await this._reconnect(true)
      }
    }
  }

  async _setupClient () {
    this.client = new pg.Client(this.opts)
    await this.client.connect()

    this.client.on('notification', (message) => {
      this._debug('[_setupClient] notification', message)

      try {
        message.payload = sjson.parse(message.payload)
      } catch (err) {
        if (err.message && err.message.includes('forbidden prototype property')) {
          this._debug('[_setupClient] prototype pollution detected in payload', message)
        }
      }

      this.ee.emit(message.channel, message.payload)
    })

    this.client.on('error', err => {
      this._debug('[_setupClient] error', err)

      if (this.reconnectRetries > this.reconnectMaxRetries) {
        this.close()
        const error = new Error('[PGPubSub]: max reconnect attempts reached, aborting', { cause: err })
        if (this.onConnectionError) {
          try { this.onConnectionError(error) } catch (e) { this._debug('[onConnectionError] callback error', e) }
          return
        }
        throw error
      }

      this._reconnect().catch(reconnectError => {
        if (this.onConnectionError) {
          try { this.onConnectionError(reconnectError) } catch (e) { this._debug('[onConnectionError] callback error', e) }
          return
        }
        process.nextTick(() => { throw reconnectError })
      })
    })

    this._debug('[_setupClient] init listeners')
    if (Object.keys(this.channels).length) {
      for (const channel in this.channels) {
        await this.client.query(`LISTEN ${format.ident(channel)}`)
      }
    }

    this.state = states.connected
    this.reconnectRetries = 0
    this._debug('[_setupClient] init listeners done')
  }

  _debug (...args) {
    /* c8 ignore next 3 */
    if (this.debug) {
      console.log(...args)
    }
  }
}

export default PGPubSub
