/* eslint-disable */
import PGPubSub from '.'

new PGPubSub({
  maxPayloadSize: 1,
  reconnectMaxRetries: 1,
  connectionString: 'connection string'
})

new PGPubSub({
  maxPayloadSize: 1,
  reconnectMaxRetries: 1,
  host: 'host',
  password: 'password',
  port: 5432,
  user: 'user'
})
