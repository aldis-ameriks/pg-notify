'use strict'

require('dotenv').config()
const PubSubPostgres = require('./lib/pg-pubsub')
const util = require('util')

const sleep = util.promisify(setTimeout)

;(async () => {
  const pubsub = new PubSubPostgres({
    reconnectMaxRetries: 100000,
    db: {
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD
    }
  })
  await pubsub.connect()

  const state = { expected: 50000, actual: 0 }

  pubsub.on('test', (_payload) => {
    state.actual++
  })

  console.time('bench')
  for (let i = 0; i < state.expected; i++) {
    await pubsub.emit({ topic: 'test', payload: 'payload' })
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
