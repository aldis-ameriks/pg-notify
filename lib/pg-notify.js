'use strict'

const EventEmitter = require('events')
const pg = require('pg')
const format = require('pg-format')
const util = require('util')
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
    this.reconnectMaxRetries = opts.reconnectMaxRetries || 10
    this.reconnectDelay = opts.reconnectDelay || 1000
    this.maxPayloadSize = opts.maxPayloadSize || 7999
    this.maxEmitRetries = opts.maxEmitRetries || 10
    this.emitThrottleDelay = opts.emitThrottleDelay || 100
    this.continuousEmitFailureThreshold = opts.continuousEmitFailureThreshold || 100

    this.state = states.init
    this.reconnectRetries = 0
    this.channels = {}
    this.queue = []
    this.flushingQueue = false
    this.continuousEmitFails = 0
  }

  emit (message, callback) {
    if (this.state === states.closing) {
      return
    }

    if (typeof message.payload === 'object') {
      message.payload = JSON.stringify(message.payload)
    }

    const payload = format.literal(message.payload)

    if (Buffer.byteLength(payload, 'utf-8') > this.maxPayloadSize) {
      throw new Error(`Payload exceeds maximum size: ${this.maxPayloadSize}`)
    }

    debug('[emit] message: ', message)
    debug('[emit] state: ', this.state)

    if (this.state !== states.connected) {
      this._insertMessageInQueue(message)
      if (callback) {
        callback()
      } else {
        return Promise.resolve()
      }
    } else {
      return this.client.query(`NOTIFY ${format.ident(message.topic)}, ${payload}`)
        .then(() => {
          this.continuousEmitFails = 0
          debug('[emit] emitted')

          // ensure queue is empty
          this._flushQueue()

          if (callback) {
            callback()
          }
        })
        .catch(err => {
          this.continuousEmitFails++
          debug('[emit] failed to emit')
          debug('[emit] state:', this.state)
          // failed to notify, add it to queue to process it later to avoid data loss
          this._insertMessageInQueue(message)

          if (this.state === states.connected) {
            console.error('[PGPubSub]: emit failed', err.message)
          }
        })
    }
  }

  on (topic, listener, callback) {
    debug('[subscribe]', topic)
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

    return this.client.query(`LISTEN ${format.ident(topic)}`)
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
      return Promise.resolve()
    }
    this.state = states.closing
    this.channels = {}
    if (this.client) {
      return this.client.end()
    }

    return Promise.resolve()
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
      debug('[_reconnect] flushing queue')
      await this._flushQueue()
    }
  }

  async _setupClient () {
    this.client = new pg.Client(this.opts.db)
    await this.client.connect()

    this.client.on('notification', (message) => {
      debug('[_setupClient] notification', message)
      this.ee.emit(message.channel, message.payload)
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

    await this._flushQueue()
  }

  _insertMessageInQueue (message) {
    debug('[_insertMessageInQueue] queue.length', this.queue.length)
    if (typeof message._retries !== 'undefined') {
      message._retries++
    } else {
      message._retries = 0
    }
    debug('[_insertMessageInQueue] message.retries', message._retries)
    this.queue.push(message)
  }

  async _flushQueue () {
    if (this.flushingQueue) {
      return
    }

    this.flushingQueue = true
    debug('[_flushQueue] flushing queue')

    while (this.queue.length) {
      if (this.state !== states.connected) {
        break
      }

      const message = this.queue.shift()

      if (message._retries && message._retries > this.maxEmitRetries) {
        // skip messages that continuously failed
        console.error('[PGPubSub]: emit failed after retries', message)
      } else {
        // start throttling retries when emits start continuously failing
        if (this.continuousEmitFails > this.continuousEmitFailureThreshold) {
          await sleep(this.emitThrottleDelay)
        }

        try {
          await this.emit(message)
        } catch (e) {
          this._insertMessageInQueue(message)
        }
      }
    }

    this.flushingQueue = false
  }
}

module.exports = PGPubSub