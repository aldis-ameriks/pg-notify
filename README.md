<h1 align="center">Welcome to pg-notify 👋</h1>
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

## Install

```sh
npm install pg-notify
```
```sh
yarn add pg-notify
```

## Usage

```js
const PGPubSub = require('pg-notify')
// import PGPubSub from 'pg-notify

;(async () => {
  const pubsub = new PGPubSub({
    db: { connectionString: 'postgres://postgres:postgres@localhost:5432/db' }
  })
  await pubsub.connect()

  await pubsub.on('test', (payload) => {
    console.log('payload: ', payload)
  })
  await pubsub.emit({ topic: 'test', payload: 'this is the payload' })
})()
```

## Contributing

Contributions, issues and feature requests are welcome!

## License

Copyright © 2020 [Aldis Ameriks](https://github.com/aldis-ameriks).<br />
This project is [MIT](https://github.com/aldis-ameriks/pg-notify/blob/main/LICENSE) licensed.

