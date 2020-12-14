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
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false
}

async function waitUntilTrue (cb) {
  while (true) {
    const result = cb()
    if (result) {
      break
    }
    await sleep(1)
  }
}

function getChannel () {
  return `channel_${Date.now()}_${Math.floor(Math.random() * Number.MAX_SAFE_INTEGER)}`
}

// suppress log errors during test runs
console.error = function () {}

test('works with await', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.channels, {})
  let emitCount = 0

  const listener = (payload) => {
    t.deepEqual(payload, 'this-is-the-payload')
    emitCount++
  }

  await pubsub.on(channel, listener)
  await pubsub.on(channel, listener)
  t.deepEqual(pubsub.channels, { [channel]: { listeners: 2 } })

  await pubsub.emit(channel, 'this-is-the-payload')
  await waitUntilTrue(() => emitCount === 2)

  await pubsub.removeListener(channel, listener)
  t.deepEqual(pubsub.channels, { [channel]: { listeners: 1 } })

  await pubsub.removeListener(channel, listener)
  t.deepEqual(pubsub.channels, {})
})

test('works when topic is in uppercase', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.channels, {})
  let emitCount = 0

  const listener = (payload) => {
    t.deepEqual(payload, 'this-is-the-payload')
    emitCount++
  }

  await pubsub.on(channel, listener)
  await pubsub.on(channel, listener)
  t.deepEqual(pubsub.channels, { [channel]: { listeners: 2 } })

  await pubsub.emit(channel, 'this-is-the-payload')
  await waitUntilTrue(() => emitCount === 2)

  await pubsub.removeListener(channel, listener)
  t.deepEqual(pubsub.channels, { [channel]: { listeners: 1 } })

  await pubsub.removeListener(channel, listener)
  t.deepEqual(pubsub.channels, {})
})

test('works with concurrent emits', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.channels, {})
  let emitCount = 0
  const expected = 1000

  const listener = (payload) => {
    t.deepEqual(payload, 'this-is-the-payload')
    emitCount++
  }

  await pubsub.on(channel, listener)
  t.deepEqual(pubsub.channels, { [channel]: { listeners: 1 } })

  for (let i = 0; i < expected; i++) {
    pubsub.emit(channel, 'this-is-the-payload')
  }
  await waitUntilTrue(() => emitCount === expected)
})

test('retries and throws when initial connection fails', async (t) => {
  const pubsub = new PGPubSub({
    reconnectMaxRetries: 10,
    db: { ...dbConfig, host: 'xxx' }
  })

  try {
    await pubsub.connect()
  } catch (e) {
    t.is(e.message, '[PGPubSub]: max reconnect attempts reached, aborting')
  }

  t.is(pubsub.reconnectRetries, 10)

  t.teardown(() => {
    pubsub.close()
  })
})

test('connection can be re-established', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({
    reconnectMaxRetries: 10000,
    db: dbConfig
  })

  await pubsub.connect()
  t.is(pubsub.state, 'connected')

  await new Promise(resolve => {
    pubsub.on(channel, (payload) => {
      t.deepEqual(payload, 'this-is-the-payload')
      resolve()
    })

    pubsub.emit(channel, 'this-is-the-payload')
  })

  const client = new pg.Client({ ...dbConfig })
  await client.connect()

  pubsub.opts.db.host = 'xxx'

  await client.query(`
      SELECT pg_terminate_backend(pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND pid <> pg_backend_pid();
  `)

  await sleep(100)

  t.is(pubsub.state, 'reconnecting')
  pubsub.opts.db.host = process.env.DB_HOST

  await waitUntilTrue(() => pubsub.state === 'connected')
  await pubsub.emit(channel, 'this-is-the-payload')

  let emitCount = 0

  await new Promise(resolve => {
    pubsub.on(channel, (payload) => {
      t.deepEqual(payload, 'this-is-the-payload')
      emitCount++
      resolve()
    })

    pubsub.emit(channel, 'this-is-the-payload')
  })
  await waitUntilTrue(() => emitCount === 1)

  t.teardown(() => {
    if (pubsub.close) {
      pubsub.close()
    }
    if (client) {
      client.end()
    }
  })
})

test.skip('connection cannot be re-established', async (t) => {
  await t.throwsAsync(async () => {
    const channel = getChannel()
    const pubsub = new PGPubSub({
      reconnectMaxRetries: 1,
      db: { ...dbConfig }
    })

    await pubsub.connect()
    t.is(pubsub.state, 'connected')

    await new Promise(resolve => {
      pubsub.on(channel, (payload) => {
        t.deepEqual(payload, 'this-is-the-payload')
        resolve()
      })

      pubsub.emit(channel, 'this-is-the-payload')
    })

    const client = new pg.Client({ ...dbConfig })
    await client.connect()

    pubsub.opts.db.host = 'xxx'
    pubsub.reconnectRetries = 100

    await client.query(`
      SELECT pg_terminate_backend(pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND pid <> pg_backend_pid();
  `)

    await sleep(1)

    await waitUntilTrue(() => pubsub.state === 'closing')

    t.teardown(() => {
      if (pubsub.close) {
        pubsub.close()
      }
      if (client) {
        client.end()
      }
    })
  })
})

test('closing while reconnecting interrupts', async (t) => {
  const pubsub = new PGPubSub({
    reconnectMaxRetries: 100,
    db: { ...dbConfig, host: 'xxx' }
  })

  // omit await
  pubsub.connect()
  await waitUntilTrue(() => pubsub.reconnectRetries > 5)
  pubsub.close()

  t.true(pubsub.reconnectRetries > 5)
  t.true(pubsub.reconnectRetries < 100)
  t.is(pubsub.state, 'closing')

  t.teardown(() => {
    pubsub.close()
  })
})

test('emit with object payload', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.channels, {})
  await new Promise(resolve => {
    pubsub.on(channel, (payload) => {
      t.deepEqual(payload, { foo: 'bar' })
      resolve()
    })

    pubsub.emit(channel, { foo: 'bar' })
  })
  t.deepEqual(pubsub.channels, { [channel]: { listeners: 1 } })
})

test('emit when not connected', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })

  t.teardown(() => {
    pubsub.close()
  })

  try {
    await pubsub.emit(channel, 'this-is-the-payload')
  } catch (e) {
    t.is(e.message, '[PGPubSub]: not connected')
  }
})

test('subscribing while not connected', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })

  t.teardown(() => {
    pubsub.close()
  })

  try {
    await pubsub.on(channel, () => {})
  } catch (e) {
    t.is(e.message, '[PGPubSub]: not connected')
  }
})

test('emit with payload exceeding max size', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  try {
    await pubsub.emit(channel, 'a'.repeat(8000))
  } catch (err) {
    t.is(err.message, '[PGPubSub]: payload exceeds maximum size: 7999')
  }
})

test('emit with payload exceeding max size after escaping', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  try {
    // payload will grow twice as large due to escaping
    await pubsub.emit(channel, '\''.repeat(5000))
  } catch (err) {
    t.is(err.message, '[PGPubSub]: payload exceeds maximum size: 7999')
  }
})

test('subscribing multiple times for same topic', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  t.deepEqual(pubsub.channels, {})

  let emitCount = 0
  await new Promise(resolve => {
    pubsub.on(channel, (payload) => {
      t.deepEqual(payload, 'this-is-the-payload')
      emitCount++ // this should run twice
      resolve()
    })

    pubsub.emit(channel, 'this-is-the-payload')
  })

  t.deepEqual(pubsub.channels, { [channel]: { listeners: 1 } })

  await new Promise(resolve => {
    pubsub.on(channel, (payload) => {
      t.deepEqual(payload, 'this-is-the-payload')
      emitCount++
      resolve()
    })

    pubsub.emit(channel, 'this-is-the-payload')
  })

  t.deepEqual(pubsub.channels, { [channel]: { listeners: 2 } })

  await waitUntilTrue(() => emitCount === 3)
})

test('attempting to subscribe when closing', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig, reconnectRetries: 10000 })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  await new Promise(resolve => {
    pubsub.on(channel, (payload) => {
      t.deepEqual(payload, 'this-is-the-payload')
      resolve()
    })

    pubsub.emit(channel, 'this-is-the-payload')
  })

  t.deepEqual(pubsub.channels, { [channel]: { listeners: 1 } })
  await pubsub.close()
  t.deepEqual(pubsub.channels, {})

  try {
    await pubsub.on(channel, (_payload) => {})
    t.fail()
  } catch (e) {
    t.is(e.message, '[PGPubSub]: not connected')
  }
})

test('removing the only listener unlistens topic', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  const listener = (payload) => {
    t.deepEqual(payload, 'this-is-the-payload')
  }

  await pubsub.on(channel, listener)
  t.deepEqual(pubsub.channels, { [channel]: { listeners: 1 } })
  await pubsub.emit(channel, 'this-is-the-payload')
  await pubsub.removeListener(channel, listener)
  t.deepEqual(pubsub.channels, {})
})

test('reduces listener count when multiple listeners', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  let emitCount = 0

  const listener = (payload) => {
    t.deepEqual(payload, 'this-is-the-payload')
    emitCount++
  }

  pubsub.on(channel, listener)
  pubsub.on(channel, listener)
  t.deepEqual(pubsub.channels, { [channel]: { listeners: 2 } })
  pubsub.emit(channel, 'this-is-the-payload')

  await waitUntilTrue(() => emitCount === 2)
  await pubsub.removeListener(channel, listener)

  t.deepEqual(pubsub.channels, { [channel]: { listeners: 1 } })
})

test('removing unknown listener', async (t) => {
  t.plan(2)

  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  t.teardown(() => {
    pubsub.close()
  })

  const listener = (payload) => {
    t.deepEqual(payload, 'this-is-the-payload')
  }

  await pubsub.on(channel, listener)
  await pubsub.emit(channel, 'this-is-the-payload')
  await pubsub.removeListener('some-other-channel', listener)
  await pubsub.emit(channel, 'this-is-the-payload')

  return new Promise(resolve => {
    setTimeout(() => {
      resolve()
    }, 100)
  })
})

test('reconnects automatically', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()

  let emitCount = 0

  await pubsub.on(channel, (payload) => {
    t.deepEqual(payload, 'this-is-the-payload')
    emitCount++
  })

  await pubsub.emit(channel, 'this-is-the-payload')

  const client = new pg.Client(dbConfig)
  await client.connect()
  await client.query(`
      SELECT pg_terminate_backend(pg_stat_activity.pid)
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND pid <> pg_backend_pid();
  `)

  await waitUntilTrue(() => pubsub.state === 'connected')
  await sleep(100)
  await pubsub.emit(channel, 'this-is-the-payload')

  await waitUntilTrue(() => emitCount === 2)

  t.teardown(() => {
    pubsub.close()
    client.end()
  })
})

test('calling close before connected', async (t) => {
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.close()
  t.pass()
})

test('calling reconnect while closing', async (t) => {
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.close()
  await pubsub._reconnect()
  t.pass()
})

test('calling close removes listeners', async (t) => {
  const channel = getChannel()
  const pubsub = new PGPubSub({ db: dbConfig })
  await pubsub.connect()
  await pubsub.on(channel, () => {})
  t.is(pubsub.ee.listenerCount(channel), 1)
  await pubsub.close()
  t.is(pubsub.ee.listenerCount(channel), 0)
})
