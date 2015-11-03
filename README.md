# Postgres async replication

This module allow you to wait for your postgres slaves to have been synced along the master from now.

## Install

```sh
npm install --save pg-async-replication
```

As postgres user, you need to install this function to give access to your database user to the `pg_stat_replication` view:
```sql
CREATE FUNCTION pg_stat_repl() RETURNS SETOF pg_stat_replication as
$$ select * from pg_stat_replication; $$
LANGUAGE sql SECURITY DEFINER;

REVOKE EXECUTE ON FUNCTION pg_stat_repl() FROM public;
GRANT EXECUTE ON FUNCTION pg_stat_repl() to DB_USER_NAME;
```

## Usage

```js
'use strict';

var pgAsyncReplicationFactory = require('pg-async-replication');

var db = require('knex')({
  client: 'pg',
  connection: {
    database: 'db_name',
    user: 'user',
    password: 'password',
    host: 'hostname'
  }
});

var pgAsyncReplication = pgAsyncReplicationFactory(db, {
  interval: 1000, // interval to check for replication state
  timeout: 5000, // timeout to reject the promise returned by `wait` (optional)
  slaves: [ // must contains slaves ip we want to wait for
    '127.0.0.1',
    '127.0.0.2',
    '127.0.0.3'
  ]
});

var now = new Date();

pgAsyncReplication.wait().then(function () {
  console.log('All database are synced from ' + now);
});

process.once('exit', function () {
  pgAsyncReplication.stop();
});
```
