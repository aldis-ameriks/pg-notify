/* eslint-disable */
import { expectError } from 'tsd'
import PGPubSub from '../..'

new PGPubSub({
  continuousEmitFailureThreshold: 1,
  emitThrottleDelay: 1,
  maxEmitRetries: 1,
  maxPayloadSize: 1,
  reconnectDelay: 1,
  reconnectMaxRetries: 1,
  emulateMqEmitterApi: true,
  db: {
    connectionString: 'connection string'
  }
})

new PGPubSub({
  continuousEmitFailureThreshold: 1,
  emitThrottleDelay: 1,
  maxEmitRetries: 1,
  maxPayloadSize: 1,
  reconnectDelay: 1,
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
