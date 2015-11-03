'use strict';

var chai = require('chai');
var sinon = require('sinon');
var knex = require('knex');
var mockDB = require('mock-knex');
var Promise = require('bluebird');

var expect = chai.expect;

var DEFAULT_INTERVAL = 1000;

describe('pgAsyncReplication', function () {
  var db;
  var tracker;
  var pgAsyncReplicationFactory;
  var clock;

  function waitQuery(data) {
    return new Promise(function (resolve) {
      process.nextTick(resolve);
    }).then(function () {
      return tracker.queries.queries.shift();
    }).then(function (query) {
      expect(query.sql).to.eql(data.sql);
      query.response(data.result);
    }).then(function () {
      return new Promise(function (resolve) {
        process.nextTick(resolve);
      });
    });
  }

  before(function () {
    db = knex({ client: 'pg' });
    mockDB.mock(db, 'knex@0.8');

    tracker = mockDB.getTracker();
    tracker.install();
  });

  after(function () {
    tracker.uninstall();
    mockDB.unmock(db);
  });

  beforeEach(function () {
    clock = sinon.useFakeTimers();

    pgAsyncReplicationFactory = require('../lib')(db);
  });

  afterEach(function () {
    clock.restore();
  });

  describe('#wait', function () {
    describe('without any slaves', function () {
      it('should resolve immediately', function (done) {
        var pgAsyncReplication = pgAsyncReplicationFactory({
          interval: DEFAULT_INTERVAL,
          slaves: []
        });

        pgAsyncReplication.wait()
        .then(done)
        .catch(done)
        .finally(pgAsyncReplication.stop.bind(pgAsyncReplication));
      });
    });

    describe('with slaves', function () {
      it('should wait for slaves to be replicated', function (done) {
        var pgAsyncReplication = pgAsyncReplicationFactory({
          interval: DEFAULT_INTERVAL,
          slaves: [
            '127.0.0.1',
            '127.0.0.2',
            '127.0.0.3'
          ]
        });

        var promise = pgAsyncReplication.wait(DEFAULT_INTERVAL * 3);

        waitQuery({
          sql: 'select pg_current_xlog_location() as "loc"',
          result: [{ loc: '9C/923EB8D8' }]
        }).then(function () {
          expect(pgAsyncReplication.pending).to.length(1);
          expect(pgAsyncReplication.pending[0].location).to.eql('9C/923EB8D8');

          clock.tick(DEFAULT_INTERVAL);

          return waitQuery({
            sql: [
              'select "t"."location"',
              'from unnest(ARRAY[?])',
              'cross join pg_stat_repl() as "stats"',
              'where "stats"."client_addr" in (?, ?, ?)',
              'and max(pg_xlog_location_diff("t"."location", "stats"."replay_location")) <= 0',
              'group by "t"."location"'
            ].join(' '),
            result: []
          });
        }).then(function () {
          expect(pgAsyncReplication.pending).to.length(1);
          expect(pgAsyncReplication.pending[0].location).to.eql('9C/923EB8D8');

          clock.tick(DEFAULT_INTERVAL);

          return waitQuery({
            sql: [
              'select "t"."location"',
              'from unnest(ARRAY[?])',
              'cross join pg_stat_repl() as "stats"',
              'where "stats"."client_addr" in (?, ?, ?)',
              'and max(pg_xlog_location_diff("t"."location", "stats"."replay_location")) <= 0',
              'group by "t"."location"'
            ].join(' '),
            result: [{
              location: '9C/923EB8D8'
            }]
          });
        }).then(function () {
          expect(pgAsyncReplication.pending).to.length(0);

          return promise;
        })
        .then(done)
        .catch(done)
        .finally(pgAsyncReplication.stop.bind(pgAsyncReplication));
      });
    });

    describe('with timeout', function () {
      it('should reject after timeout', function (done) {
        var pgAsyncReplication = pgAsyncReplicationFactory({
          interval: DEFAULT_INTERVAL,
          slaves: [
            '127.0.0.1',
            '127.0.0.2',
            '127.0.0.3'
          ]
        });

        var promise = pgAsyncReplication.wait(DEFAULT_INTERVAL * 3);

        waitQuery({
          sql: 'select pg_current_xlog_location() as "loc"',
          result: [{ loc: '9C/923EB8D8' }]
        }).then(function () {
          clock.tick(DEFAULT_INTERVAL * 3);
          return promise;
        })
        .then(function () {
          done('pgAsyncReplication.wait did not reject');
        })
        .catch(function () {
          expect(pgAsyncReplication.pending).to.length(0);

          done();
        })
        .finally(pgAsyncReplication.stop.bind(pgAsyncReplication));
      });
    });
  });
});
