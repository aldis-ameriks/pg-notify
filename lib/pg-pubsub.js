'use strict'

const EventEmitter = require('events')
const pg = require('pg')
const format = require('pg-format')
const util = require('util')
const sjson = require('secure-json-parse')
const debug = require('debug')('pg-pubsub')

const sleep = util.promisify(setTimeout)

class PGPubSub {
  constructor (opts) {
    this.opts = opts
    this.ee = new EventEmitter()
    this.ee.setMaxListeners(0)
    this.reconnectMaxRetries = opts.reconnectMaxRetries || 30
    this.reconnectDelay = opts.reconnectDelay || 2000
    this.heartbeatInterval = opts.heartbeatInterval || 5000
    this.maxPayloadSize = opts.maxPayloadSize || 7999

    this.channels = {}
    this.closing = false
    this.reconnecting = false
    this.connected = false
    this.queue = []
    this.heartbeat = undefined
    this.reconnectRetries = 0
  }

  emit (message, callback) {
    if (this.closing) {
      return
    }

    if (typeof message.payload === 'object') {
      message.payload = JSON.stringify(message.payload)
    }

    if (Buffer.byteLength(message.payload, 'utf-8') > this.maxPayloadSize) {
      throw new Error(`Payload exceeds maximum size: ${this.maxPayloadSize}`)
    }

    debug('[emit] message: ', message)
    debug('[emit] connected: ', this.connected)

    if (!this.connected) {
      this.queue.push(message)
      if (callback) {
        callback()
      }
      return
    }

    this.client.query(`NOTIFY ${format.ident(message.topic)}, ${format.literal(message.payload)}`)
      .then(() => {
        debug('[emit] emitted')
        this._flushQueue()
        if (callback) {
          callback()
        }
      })
      .catch(err => {
        debug('[emit] failed to emit')
        debug('[emit] reconnecting:', this.reconnecting)
        debug('[emit] connected:', this.connected)
        // failed to notify, add it to queue to process it later to avoid data loss
        this.queue.push(message)

        if (!this.closing && !this.reconnecting && this.connected) {
          console.error('[PGPubSub]: emit failed', err.message)
        }
      })
  }

  emitAsync (message) {
    return new Promise(resolve => {
      this.emit(message, resolve)
    })
  }

  on (topic, listener, callback) {
    if (this.closing) {
      return
    }

    // needed to support this as drop-in replacement for mqemitter
    const handler = (value) => {
      let payload = value
      try {
        payload = sjson.parse(value)
      } catch {}

      listener({ payload }, () => {})
    }

    if (this.channels[topic]) {
      this.ee.on(topic, handler)
      this.channels[topic].listeners++
      if (callback) {
        callback()
      }
      return
    }

    this.ee.on(topic, handler)
    this.channels[topic] = { listeners: 1 }

    if (!this.connected) {
      return
    }

    this.client.query(`LISTEN ${format(topic)}`)
      .then(() => {
        if (callback) {
          callback()
        }
      })
      .catch((err) => {
        if (!this.closing && !this.reconnecting && this.connected) {
          console.error('[PGPubSub]: subscribe failed', err.message)
        }
      })
  }

  removeListener (topic, handler, callback) {
    if (!this.channels[topic]) {
      callback()
      return
    }

    this.ee.removeListener(topic, handler)
    this.channels[topic].listeners--

    if (this.channels[topic].listeners === 0) {
      delete this.channels[topic]
      if (!this.connected) {
        return
      }

      this.client.query(`UNLISTEN ${format.ident(topic)}`)
        .then(() => {
          if (callback) {
            callback()
          }
        })
        .catch(err => {
          if (!this.closing && !this.reconnecting && this.connected) {
            console.error('[PGPubSub]: removeListener failed', err.message)
          }
        })
    } else {
      if (callback) {
        callback()
      }
    }
  }

  async connect () {
    this.reconnectRetries = 0
    this.closing = false
    this.reconnecting = false
    this.connected = false

    try {
      await this._setupClient()
    } catch (e) {
      this.connected = false
      await this._reconnect()
    }
  }

  close () {
    if (this.closing) {
      return
    }
    this.closing = true
    this.channels = {}
    if (this.heartbeat) {
      clearInterval(this.heartbeat)
    }
    if (this.client) {
      return this.client.end()
    }
  }

  async _reconnect () {
    if (this.reconnecting || this.closing) {
      return
    }

    this.connected = false
    debug('[_reconnect] connected: ', this.connected)

    if (this.reconnectRetries > 5) {
      await sleep(this.reconnectDelay)
    }

    this.reconnecting = true
    this.reconnectRetries++

    try {
      this.client.end()
      await this._setupClient()
    } catch (err) {
      if (this.reconnectRetries >= this.reconnectMaxRetries) {
        this.close()
        throw new Error('[PGPubSub]: Max reconnect attempts reached, aborting', err)
      }
      if (!this.connected) {
        this.reconnecting = false
        await this._reconnect()
      }
    }

    if (this.connected && !this.reconnecting) {
      this._flushQueue()
    }
  }

  async _setupClient () {
    this.client = new pg.Client(this.opts.db)
    await this.client.connect()

    this._setupHeartbeat()

    this.client.on('notification', ({ channel, payload }) => {
      this.ee.emit(channel, payload)
    })

    this.client.on('error', err => {
      debug('[_setupClient] error')
      this.connected = false

      if (this.reconnectRetries > this.reconnectMaxRetries) {
        this.close()
        throw new Error('[PGPubSub]: Max reconnect attempts reached, aborting', err)
      }

      this._reconnect()
    })

    debug('[_setupClient] init listeners')
    if (Object.keys(this.channels).length) {
      for (const channel in this.channels) {
        await this.client.query(`LISTEN ${format.ident(channel)}`)
      }
    }

    this.connected = true
    this.reconnecting = false
    this.reconnectRetries = 0
    debug('[_setupClient] init listeners done')

    this._flushQueue()
  }

  _setupHeartbeat () {
    if (this.heartbeat) {
      clearInterval(this.heartbeat)
    }
    this.heartbeat = setInterval(() => {
      this.client.query('select 1 as result').then(result => {
        if (result.rows[0].result !== 1) {
          console.warn('Unexpected result from heartbeat:', result.rows[0])
        }
      }).catch(() => {
        console.warn('Heartbeat failed, reconnecting')
        this._reconnect()
      })
    }, this.heartbeatInterval)
  }

  _flushQueue () {
    if (this.queue.length) {
      for (const message of this.queue) {
        this.emit(message)
      }
      this.queue = []
    }
  }
}

module.exports = PGPubSub
