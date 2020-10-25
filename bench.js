'use strict'

require('dotenv').config()
const assert = require('assert').strict
const PubSubPostgres = require('./lib/pg-pubsub')

;(async () => {
  const pubsub = new PubSubPostgres({
    db: {
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
    }
  })
  await pubsub.connect()

  let messages = 0
  pubsub.on('test', (_payload) => {
    messages++
  })

  console.time('start')
  const count = 10000
  for (let i = 0; i < count; i++) {
    await pubsub.emit({ topic: 'test', payload: 'payload' })
  }
  console.timeEnd('start')
  assert.equal(messages, count)
  await pubsub.close()
})()
