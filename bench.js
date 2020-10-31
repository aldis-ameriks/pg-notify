'use strict'

require('dotenv').config()
const PGPubSub = require('./lib/pg-notify')
const util = require('util')
const pg = require('pg')
const assert = require('assert').strict

const sleep = util.promisify(setTimeout)

async function runPgNotifyBench () {
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

  console.time('bench - pg notify')
  for (let i = 0; i < state.expected; i++) {
    await pubsub.emit('test', 'payload')
    if (i % 5000 === 0) {
      console.log('emit counter: ', i)
    }
  }

  while (true) {
    console.log('processed messages: ', state.actual)
    if (state.actual === state.expected) {
      break
    }
    await sleep(1000)
  }

  console.timeEnd('bench - pg notify')
  await pubsub.close()
}

async function runPgRawBench () {
  const client = new pg.Client({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false
  })
  await client.connect()

  const state = { expected: 50000, actual: 0 }

  await client.query('LISTEN test')

  client.on('notification', (notification) => {
    state.actual++
    assert.equal(notification.payload, 'payload')
  })

  console.time('bench - pg raw')
  for (let i = 0; i < state.expected; i++) {
    await client.query("NOTIFY test, 'payload'")
    if (i % 5000 === 0) {
      console.log('emit counter: ', i)
    }
  }

  while (true) {
    console.log('processed messages: ', state.actual)
    if (state.actual === state.expected) {
      break
    }
    await sleep(1000)
  }

  console.timeEnd('bench - pg raw')
  await client.end()
}

;(async () => {
  await runPgNotifyBench()
  await runPgRawBench()
})()
