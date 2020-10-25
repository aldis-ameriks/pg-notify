'use strict'

const EventEmitter = require('events')
const pg = require('pg')
const format = require('pg-format')
const util = require('util')
const sjson = require('secure-json-parse')
const debug = require('debug')('pg-pubsub')

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
    this.reconnectMaxRetries = opts.reconnectMaxRetries || 10
    this.reconnectDelay = opts.reconnectDelay || 2000
    this.maxPayloadSize = opts.maxPayloadSize || 7999


    this.channels = {}
    this.state = states.init
    this.queue = []
    this.reconnectRetries = 0
  }

  emit (message, callback) {
    if (this.state === states.closing) {
      return
    }

    if (typeof message.payload === 'object') {
      message.payload = JSON.stringify(message.payload)
    }

    if (Buffer.byteLength(message.payload, 'utf-8') > this.maxPayloadSize) {
      throw new Error(`Payload exceeds maximum size: ${this.maxPayloadSize}`)
    }

    debug('[emit] message: ', message)
    debug('[emit] state: ', this.state)

    if (this.state !== states.connected) {
      this.queue.push(message)
      if (callback) {
        callback()
        return
      } else {
        return Promise.resolve()
      }
    }

    return this.client.query(`NOTIFY ${format.ident(message.topic)}, ${format.literal(message.payload)}`)
      .then(() => {
        debug('[emit] emitted')
        this._flushQueue()
        if (callback) {
          callback()
        }
      })
      .catch(err => {
        debug('[emit] failed to emit')
        debug('[emit] state:', this.state)
        // failed to notify, add it to queue to process it later to avoid data loss
        this.queue.push(message)

        if (this.state === states.connected) {
          console.error('[PGPubSub]: emit failed', err.message)
        }
      })
  }

  on (topic, listener, callback) {
    if (this.state === states.closing) {
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
        return
      } else {
        return Promise.resolve()
      }
    }

    this.ee.on(topic, handler)
    this.channels[topic] = { listeners: 1 }

    if (this.state !== states.connected) {
      return
    }

    return this.client.query(`LISTEN ${format(topic)}`)
      .then(() => {
        if (callback) {
          callback()
        }
      })
      .catch((err) => {
        if (this.state === states.connected) {
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
      if (this.state !== states.connected) {
        if (callback) {
          callback()
          return
        } else {
          return Promise.resolve()
        }
      }

      return this.client.query(`UNLISTEN ${format.ident(topic)}`)
        .then(() => {
          if (callback) {
            callback()
          }
        })
        .catch(err => {
          if (this.state === states.connected) {
            console.error('[PGPubSub]: removeListener failed', err.message)
          }
        })
    } else {
      if (callback) {
        callback()
      } else {
        return Promise.resolve()
      }
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

  close () {
    if (this.state === states.closing) {
      return
    }
    this.state = states.closing
    this.channels = {}
    if (this.client) {
      return this.client.end()
    }
  }

  async _reconnect (force) {
    debug('[_reconnect] state: ', this.state)

    if ([states.reconnecting, states.closing].includes(this.state) && !force) {
      return
    }

    this.state = states.reconnecting
    this.reconnectRetries++

    if (this.reconnectRetries > 5) {
      await sleep(this.reconnectDelay)
    }


    try {
      this.client.end()
      await this._setupClient()
    } catch (err) {
      if (this.reconnectRetries >= this.reconnectMaxRetries) {
        this.close()
        throw new Error('[PGPubSub]: Max reconnect attempts reached, aborting', err)
      }
      if (![states.closing, states.connected].includes((this.state))) {
        await this._reconnect(true)
      }
    }

    if (this.state === states.connected) {
      this._flushQueue()
    }
  }

  async _setupClient () {
    this.client = new pg.Client(this.opts.db)
    await this.client.connect()

    this.client.on('notification', ({ channel, payload }) => {
      this.ee.emit(channel, payload)
    })

    this.client.on('error', err => {
      debug('[_setupClient] error')

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

    this.state = states.connected
    this.reconnectRetries = 0
    debug('[_setupClient] init listeners done')

    this._flushQueue()
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
