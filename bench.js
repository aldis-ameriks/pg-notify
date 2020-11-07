'use strict'

require('dotenv').config()
const util = require('util')
const pg = require('pg')
const assert = require('assert').strict
const Benchmark = require('benchmark')

const PGPubSub = require('./lib/pg-notify')
const suite = new Benchmark.Suite()
const sleep = util.promisify(setTimeout)
const iterations = 100

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

  const state = { expected: iterations, actual: 0 }

  pubsub.on('test', (payload) => {
    state.actual++
    assert.equal(payload, 'payload')
  })

  for (let i = 0; i < state.expected; i++) {
    await pubsub.emit('test', 'payload')
  }

  while (true) {
    if (state.actual === state.expected) {
      break
    }
    await sleep(1)
  }

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

  const state = { expected: iterations, actual: 0 }

  await client.query('LISTEN test')

  client.on('notification', (notification) => {
    state.actual++
    assert.equal(notification.payload, 'payload')
  })

  for (let i = 0; i < state.expected; i++) {
    await client.query("NOTIFY test, 'payload'")
  }

  while (true) {
    if (state.actual === state.expected) {
      break
    }
    await sleep(1)
  }

  await client.end()
}

(async () => {
  suite
    .add('pg', {
      defer: true,
      minSamples: 50,
      fn (deferred) {
        runPgRawBench().then(() => deferred.resolve())
      }
    })
    .add('pg-notify', {
      defer: true,
      minSamples: 50,
      fn (deferred) {
        runPgNotifyBench().then(() => deferred.resolve())
      }
    })
    .on('cycle', function (event) {
      console.log(String(event.target))
    })
    .on('complete', function () {
      console.log('Fastest is ' + this.filter('fastest').map('name'))
    })
    .run({ async: true })
})()
