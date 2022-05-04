'use strict'

const EventEmitter = require('events')
const util = require('util')
const pg = require('pg')
const format = require('pg-format')
const sjson = require('secure-json-parse')
const debug = require('debug')('pg-notify')

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
    this.maxPayloadSize = opts.maxPayloadSize || 7999 // default on a standard pg installation

    this.state = states.init
    this.reconnectRetries = 0
    this.channels = {}
  }

  async emit (channel, payload) {
    if (this.state !== states.connected) {
      throw new Error('[PGPubSub]: not connected')
    }

    if (typeof payload === 'object') {
      payload = JSON.stringify(payload)
    }

    const parsedPayload = format.literal(payload)

    if (Buffer.byteLength(parsedPayload, 'utf-8') > this.maxPayloadSize) {
      throw new Error(`[PGPubSub]: payload exceeds maximum size: ${this.maxPayloadSize}`)
    }

    debug('[emit] channel: ', channel)
    debug('[emit] payload: ', payload)
    debug('[emit] state: ', this.state)

    return this.client.query(`NOTIFY ${format.ident(channel)}, ${parsedPayload}`)
  }

  async on (channel, listener) {
    debug('[subscribe]', channel)
    if (this.state !== states.connected) {
      throw new Error('[PGPubSub]: not connected')
    }

    if (this.channels[channel]) {
      this.ee.on(channel, listener)
      this.channels[channel].listeners++
      return
    }

    this.ee.on(channel, listener)
    this.channels[channel] = { listeners: 1 }

    return this.client.query(`LISTEN ${format.ident(channel)}`)
  }

  async removeListener (channel, listener) {
    if (!this.channels[channel]) {
      return
    }

    this.ee.removeListener(channel, listener)
    this.channels[channel].listeners--

    if (this.channels[channel].listeners === 0) {
      delete this.channels[channel]
      return this.client.query(`UNLISTEN ${format.ident(channel)}`)
    }
  }

  async connect () {
    this.reconnectRetries = 0
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
    debug('[_reconnect] state: ', this.state)

    if (!this.reconnectMaxRetries) {
      this.close()
      return
    }

    if ([states.reconnecting, states.closing].includes(this.state) && !force) {
      return
    }

    this.state = states.reconnecting
    this.reconnectRetries++

    try {
      this.client.end()
      await this._setupClient()
    } catch (err) {
      if (this.reconnectRetries >= this.reconnectMaxRetries) {
        this.close()
        throw new Error('[PGPubSub]: max reconnect attempts reached, aborting', err)
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
      debug('[_setupClient] notification', message)

      try {
        message.payload = sjson.parse(message.payload)
      } catch {}

      this.ee.emit(message.channel, message.payload)
    })

    this.client.on('error', err => {
      debug('[_setupClient] error')

      if (this.reconnectRetries > this.reconnectMaxRetries) {
        this.close()
        throw new Error('[PGPubSub]: max reconnect attempts reached, aborting', err)
      }

      this._reconnect()
    })

    debug('[_setupClient] init listeners')
    if (Object.keys(this.channels).length) {
      for (const channel in this.channels) {
        await this.client.query(`LISTEN ${format.ident(channel)}`)
      }
    }

    this.state = states.connected
    this.reconnectRetries = 0
    debug('[_setupClient] init listeners done')
  }
}

module.exports = PGPubSub
