'use strict'

require('dotenv').config()
const PGPubSub = require('./lib/pg-notify')
const util = require('util')
const assert = require('assert').strict

const sleep = util.promisify(setTimeout)

;(async () => {
  const pubsub = new PGPubSub({
    reconnectMaxRetries: 100000,
    db: {
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_DATABASE,
      ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false
    }
  })
  await pubsub.connect()

  const state = { expected: 50000, actual: 0 }

  pubsub.on('test', (payload) => {
    state.actual++
    assert.equal(payload, 'payload')
  })

  console.time('bench')
  for (let i = 0; i < state.expected; i++) {
    await pubsub.emit('test', 'payload')
    if (i % 1000 === 0) {
      console.log('emit counter: ', i)
    }
  }
  console.timeEnd('bench')

  while (true) {
    console.log('processed messages: ', state.actual)
    if (state.actual === state.expected) {
      break
    }
    await sleep(1000)
  }

  await pubsub.close()
})()
