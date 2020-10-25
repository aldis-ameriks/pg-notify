'use strict'

require('dotenv').config()

const test = require('ava')
const pg = require('pg')
const util = require('util')
const PGPubSub = require('../lib/pg-notify')

const sleep = util.promisify(setTimeout)

const dbConfig = {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD
}

async function waitUntilStateIsSatisfied (state) {
  while (true) {
    let actual
    if (typeof state.actual === 'function') {
      actual = state.actual()
    } else {
      actual = state.actual
    }

    if (typeof state.expected === 'number' && actual > state.expected) {
      throw new Error(`expected: ${state.expected}, actual: ${actual}`)
    }

    if (actual === state.expected) {
      break
    }

    await sleep(1)
  }
}

// suppress log errors during test runs
console.error = function () {}

test('works with await', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.channels, {})
  const state = { expected: 2, actual: 0 }

  const listener = (payload) => {
    t.deepEqual(payload, { payload: 'this-is-the-payload' })
    state.actual++
  }

  await pubsub.on('channel', listener)
  await pubsub.on('channel', listener)
  t.deepEqual(pubsub.channels, { channel: { listeners: 2 } })

  await pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })
  await waitUntilStateIsSatisfied(state)

  await pubsub.removeListener('channel', listener)
  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })

  await pubsub.removeListener('channel', listener)
  t.deepEqual(pubsub.channels, {})
})

test('works with callbacks', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.channels, {})
  const state = { expected: 2, actual: 0 }

  const listener = (payload) => {
    t.deepEqual(payload, { payload: 'this-is-the-payload' })
    state.actual++
  }

  await new Promise(resolve => {
    pubsub.on('channel', listener, resolve)
  })
  await new Promise(resolve => {
    pubsub.on('channel', listener, resolve)
  })
  t.deepEqual(pubsub.channels, { channel: { listeners: 2 } })

  await new Promise(resolve => {
    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' }, resolve)
  })

  await waitUntilStateIsSatisfied(state)

  await new Promise(resolve => {
    pubsub.removeListener('channel', listener, resolve)
  })
  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })

  await new Promise(resolve => {
    pubsub.removeListener('channel', listener, resolve)
  })
  t.deepEqual(pubsub.channels, {})
})

test('retries and throws when initial connection fails', async (t) => {
  t.timeout(3000)

  const pubsub = new PGPubSub({
    reconnectMaxRetries: 10,
    reconnectDelay: 10,
    db: { ...dbConfig, host: 'xxx' }
  })

  try {
    await pubsub.connect()
  } catch (e) {
    t.is(e.message, '[PGPubSub]: Max reconnect attempts reached, aborting')
  }

  t.is(pubsub.reconnectRetries, 10)

  t.teardown(() => {
    pubsub.close()
  })
})

test('connection can be re-established', async (t) => {
  t.timeout(5000)

  const pubsub = new PGPubSub({
    reconnectMaxRetries: 50,
    reconnectDelay: 100,
    heartbeatInterval: 1000,
    db: dbConfig
  })

  await pubsub.connect()
  t.is(pubsub.state, 'connected')

  await new Promise(resolve => {
    pubsub.on('channel', (payload) => {
      t.deepEqual(payload, { payload: 'this-is-the-payload' })
      resolve()
    })

    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })
  })

  const client = new pg.Client({ ...dbConfig })
  await client.connect()

  pubsub.opts.db.host = 'xxx'

  await client.query(`
    SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE datname = current_database() AND pid <> pg_backend_pid();
  `)

  await sleep(100)

  t.is(pubsub.state, 'reconnecting')
  pubsub.opts.db.host = 'localhost'

  let state = { expected: 'connected', actual: () => pubsub.state }
  await waitUntilStateIsSatisfied(state)

  pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })

  state = { expected: 1, actual: 0 }

  await new Promise(resolve => {
    pubsub.on('channel', (payload) => {
      t.deepEqual(payload, { payload: 'this-is-the-payload' })
      state.actual++
      resolve()
    })

    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })
  })
  await waitUntilStateIsSatisfied(state)

  t.teardown(() => {
    if (pubsub.close) {
      pubsub.close()
    }
    if (client) {
      client.end()
    }
  })
})

test('closing while reconnecting interrupts', async (t) => {
  t.timeout(2000)

  const pubsub = new PGPubSub({
    reconnectMaxRetries: 10,
    reconnectDelay: 2000,
    db: { ...dbConfig, host: 'xxx' }
  })

  // omit await
  pubsub.connect()
  const state = { expected: 6, actual: () => pubsub.reconnectRetries }
  await waitUntilStateIsSatisfied(state, pubsub)
  pubsub.close()

  t.true(pubsub.reconnectRetries > 5)
  t.true(pubsub.reconnectRetries < 10)
  t.is(pubsub.state, 'closing')

  t.teardown(() => {
    pubsub.close()
  })
})

test('emit with callback', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  return new Promise(resolve => {
    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' }, () => {
      t.pass()
      resolve()
    })
  })
})

test('emit with object payload', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.channels, {})
  await new Promise(resolve => {
    pubsub.on('channel', (payload) => {
      t.deepEqual(payload, { payload: { foo: 'bar' } })
      resolve()
    })

    pubsub.emit({ topic: 'channel', payload: { foo: 'bar' } })
  })
  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })
})

test('emit when not connected', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.queue, [])
  await new Promise(resolve => {
    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' }, resolve)
  })

  await pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })

  t.deepEqual(pubsub.queue, [
    { topic: 'channel', payload: 'this-is-the-payload', _retries: 0 },
    { topic: 'channel', payload: 'this-is-the-payload', _retries: 0 }
  ])

  const state = { expected: 2, actual: 0 }
  pubsub.on('channel', (payload) => {
    t.deepEqual(payload, { payload: 'this-is-the-payload' })
    state.actual++
  })

  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })

  await pubsub.connect()
  await waitUntilStateIsSatisfied(state)
})

// TODO: Removing listeners is not working as expected, because new handler is created on subscribe
test.skip('subscribing and unsubscribing while not connected', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.queue, [])
  await new Promise(resolve => {
    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' }, resolve)
  })
  t.deepEqual(pubsub.queue, [{ topic: 'channel', payload: 'this-is-the-payload', _retries: 0 }])

  const state = { expected: 2, actual: 0 }

  const listener = (payload) => {
    t.deepEqual(payload, { payload: 'this-is-the-payload' })
    state.actual++
  }

  pubsub.on('channel', listener)
  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })

  await pubsub.removeListener('channel', listener)
  t.deepEqual(pubsub.channels, {})

  pubsub.on('channel', listener)
  pubsub.on('channel', listener)
  t.deepEqual(pubsub.channels, { channel: { listeners: 2 } })

  await pubsub.removeListener('channel', listener)
  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })

  await new Promise(resolve => {
    pubsub.removeListener('channel', listener, resolve)
  })
  t.deepEqual(pubsub.channels, {})

  pubsub.on('channel', listener)
  pubsub.on('channel', listener)
  t.deepEqual(pubsub.channels, { channel: { listeners: 2 } })

  await pubsub.connect()
  await waitUntilStateIsSatisfied(state)
})

test('emit with payload exceeding max size', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  try {
    pubsub.emit({ topic: 'channel', payload: 'a'.repeat(8000) })
  } catch (err) {
    t.is(err.message, 'Payload exceeds maximum size: 7999')
  }
})

test('emit with payload exceeding max size after escaping', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  try {
    // payload will grow twice as large due to escaping
    pubsub.emit({ topic: 'channel', payload: '\''.repeat(5000) })
  } catch (err) {
    t.is(err.message, 'Payload exceeds maximum size: 7999')
  }
})

test('continuously failing messages are dropped from queue', async (t) => {
  t.timeout(1000)

  // intentionally increase max payload size above standard 8000 bytes
  const pubsub = new PGPubSub({
    db: dbConfig,
    maxPayloadSize: 20000,
    continuousEmitFailureThreshold: 3,
    emitThrottleDelay: 10
  })

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.queue, [])
  // emit payload that's too large for a standard configuration of postgres
  // since we're not connected yet, it will just be added in message queue
  await pubsub.emit({ topic: 'channel', payload: 'a'.repeat(10000) })
  t.deepEqual(pubsub.queue, [{ topic: 'channel', payload: 'a'.repeat(10000), _retries: 0 }])

  await pubsub.on('channel', (_payload) => {
    t.fail()
  })

  // after we connect, the queue should get flushed with pg throwing error due to payload size
  await pubsub.connect()

  const state = { expected: 0, actual: () => pubsub.queue.length }
  await waitUntilStateIsSatisfied(state)
  await sleep(100) // to ensure message is never received in subscribe
})

test('aborts flushing queue when connection is lost', async (t) => {
  t.timeout(1000)

  // intentionally increase max payload size above standard 8000 bytes
  const pubsub = new PGPubSub({
    db: dbConfig,
    maxPayloadSize: 20000
  })

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.queue, [])
  // emit payload that's too large for a standard configuration of postgres
  // since we're not connected yet, it will just be added in message queue
  await pubsub.emit({ topic: 'channel', payload: 'a'.repeat(10000) })
  t.deepEqual(pubsub.queue, [{ topic: 'channel', payload: 'a'.repeat(10000), _retries: 0 }])

  await pubsub.on('channel', (_payload) => {
    t.fail()
  })

  // after we connect, the queue should get flushed with pg throwing error due to payload size
  pubsub.connect()
  let state = { expected: true, actual: () => pubsub.flushingQueue }
  await waitUntilStateIsSatisfied(state)

  pubsub.close()
  state = { expected: false, actual: () => pubsub.flushingQueue }
  await waitUntilStateIsSatisfied(state)

  // the message is still in the queue
  t.is(pubsub.queue.length, 1)
  await sleep(100) // to ensure message is never received in subscribe
})

test('subscribing multiple times for same topic', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.channels, {})

  const state = { expected: 3, actual: 0 }
  await new Promise(resolve => {
    pubsub.on('channel', (payload) => {
      t.deepEqual(payload, { payload: 'this-is-the-payload' })
      state.actual++ // this should run twice
      resolve()
    })

    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })
  })

  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })

  await new Promise(resolve => {
    pubsub.on('channel', (payload) => {
      t.deepEqual(payload, { payload: 'this-is-the-payload' })
      state.actual++
      resolve()
    })

    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })
  })

  t.deepEqual(pubsub.channels, { channel: { listeners: 2 } })

  await waitUntilStateIsSatisfied(state)
})

test('attempting to subscribe when closing', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  await new Promise(resolve => {
    pubsub.on('channel', (payload) => {
      t.deepEqual(payload, { payload: 'this-is-the-payload' })
      resolve()
    })

    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })
  })

  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })
  pubsub.close()
  t.deepEqual(pubsub.channels, {})
  pubsub.on('channel', (_payload) => {})
  t.deepEqual(pubsub.channels, {})
})

test('removing the only listener unlistens topic', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  await new Promise(resolve => {
    const listener = (payload) => {
      t.deepEqual(payload, { payload: 'this-is-the-payload' })
      pubsub.removeListener('channel', listener, resolve)
    }

    pubsub.on('channel', listener)
    t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })
    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })
  })

  t.deepEqual(pubsub.channels, {})
})

test('reduces listener count when multiple listeners', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  const state = { expected: 2, actual: 0 }

  const listener = (payload) => {
    t.deepEqual(payload, { payload: 'this-is-the-payload' })
    state.actual++
  }

  pubsub.on('channel', listener)
  pubsub.on('channel', listener)
  t.deepEqual(pubsub.channels, { channel: { listeners: 2 } })
  pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })

  await waitUntilStateIsSatisfied(state)

  await new Promise(resolve => {
    pubsub.removeListener('channel', listener, resolve)
  })

  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })
})

test('callback is called when subscribing', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  const state = { expected: 2, actual: 0 }

  const listener = (payload) => {
    t.deepEqual(payload, { payload: 'this-is-the-payload' })
    state.actual++
  }

  await new Promise(resolve => {
    pubsub.on('channel', listener, resolve)
  })

  await new Promise(resolve => {
    pubsub.on('channel', listener, resolve)
  })

  t.deepEqual(pubsub.channels, { channel: { listeners: 2 } })
  pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })

  await waitUntilStateIsSatisfied(state)

  await new Promise(resolve => {
    pubsub.removeListener('channel', listener, resolve)
  })

  t.deepEqual(pubsub.channels, { channel: { listeners: 1 } })
})

test('removing unknown listener', async (t) => {
  t.timeout(1000)
  t.plan(2)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  await new Promise(resolve => {
    const listener = (payload) => {
      t.deepEqual(payload, { payload: 'this-is-the-payload' })
      pubsub.removeListener('some-other-channel', listener, resolve)
    }

    pubsub.on('channel', listener)
    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })
  })

  return new Promise(resolve => {
    pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })
    setTimeout(() => {
      resolve()
    }, 100)
  })
})

test('reconnects automatically', async (t) => {
  t.timeout(1000)

  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  const state = { expected: 2, actual: 0 }

  pubsub.on('channel', (payload) => {
    t.deepEqual(payload, { payload: 'this-is-the-payload' })
    state.actual++
  })

  pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })

  const client = new pg.Client(dbConfig)
  await client.connect()

  await client.query(`
    SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE datname = current_database() AND pid <> pg_backend_pid();
  `)

  pubsub.emit({ topic: 'channel', payload: 'this-is-the-payload' })

  await waitUntilStateIsSatisfied(state)

  t.teardown(() => {
    pubsub.close()
    client.end()
  })
})

test('calling close before connected', async (t) => {
  t.timeout(1000)
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.close()
  t.pass()
})
