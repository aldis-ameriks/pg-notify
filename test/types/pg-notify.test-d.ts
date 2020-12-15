/* eslint-disable */
import { expectError } from 'tsd'
import PGPubSub from '../..'

new PGPubSub({
  maxPayloadSize: 1,
  reconnectMaxRetries: 1,
  db: {
    connectionString: 'connection string'
  }
})

new PGPubSub({
  maxPayloadSize: 1,
  reconnectMaxRetries: 1,
  db: {
    host: 'host',
    password: 'password',
    port: 5432,
    user: 'user'
  }
})

// db option is mandatory
expectError(new PGPubSub({}))
