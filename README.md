<h1 align="center">pg-notify</h1>
<p>
    <a href="https://www.npmjs.com/package/pg-notify" target="_blank">
        <img alt="Version" src="https://img.shields.io/npm/v/pg-notify.svg">
    </a>
    <a href="https://github.com/aldis-ameriks/pg-notify/graphs/commit-activity" target="_blank">
        <img alt="Maintenance" src="https://img.shields.io/badge/Maintained%3F-yes-green.svg" />
    </a>
    <a href="https://github.com/aldis-ameriks/pg-notify/blob/main/LICENSE" target="_blank">
        <img alt="License: MIT" src="https://img.shields.io/github/license/aldis-ameriks/pg-notify" />
    </a>
    <a href='https://coveralls.io/github/aldis-ameriks/pg-notify?branch=main'>
        <img src='https://coveralls.io/repos/github/aldis-ameriks/pg-notify/badge.svg?branch=main' alt='Coverage Status' />
    </a>
    <a href="https://github.com/aldis-ameriks/pg-notify/workflows/CI/badge.svg" target="_blank">
        <img alt="CI" src="https://github.com/aldis-ameriks/pg-notify/workflows/CI/badge.svg" />
    </a>
</p>

> Postgres PubSub client using NOTIFY/LISTEN


## Features
- Auto reconnect
- Payload size validation
- Channel and payload sanitization


## Install

```sh
npm install pg-notify
```
```sh
yarn add pg-notify
```

```sh
pnpm add pg-notify
```

## Usage

> PGPubSub accepts the same config as [pg](https://github.com/brianc/node-postgres).

```js
import PGPubSub from 'pg-notify'
//const PGPubSub = require('pg-notify')

;(async () => {
  const pubsub = new PGPubSub({ 
    connectionString: 'postgres://postgres:postgres@localhost:5432/db'
  })

  await pubsub.connect()

  await pubsub.on('test', (payload) => {
    console.log('payload: ', payload)
  })

  await pubsub.emit('test', 'this is the payload')
  await pubsub.emit('test', { foo: 'bar' })

  await pubsub.close()
})()
```

## API

### new PubSub(options)
- `options` (`object`) Configuration options for pg-notify pubsub instance. Accepts same options as [pg](https://github.com/brianc/node-postgres) with few custom ones described below.
    - reconnectMaxRetries (`number`) Maximum number of reconnect attempts after losing connection. Pass `0` to disable reconnecting. Default: `10`.
    - maxPayloadSize (`number`) Maximum payload size, exceeding given size will throw an error. Default: `7999` ([In the default configuration it must be shorter than 8000 bytes.](https://www.postgresql.org/docs/current/sql-notify.html)).

### emit(channel, payload)
- `channel` (`string`)
- `payload` (`string` or `object`)

### on(channel, listener)
- `channel` (`string`)
- `listener` (`function` accepting single argument `payload`)

### removeListener(listener)
- `listener` (`function` accepting single argument `payload`)

### close()
### connect()

## Contributing

Contributions, issues and feature requests are welcome!

## License

Copyright © 2020 [Aldis Ameriks](https://github.com/aldis-ameriks).<br />
This project is [MIT](https://github.com/aldis-ameriks/pg-notify/blob/main/LICENSE) licensed.

