"use strict";

var _ = require("lodash");
var Promise = require("bluebird");

var getMasterLocation = function(knex) {
  return knex
    .select(knex.raw('pg_current_wal_lsn() as "loc"'))
    .then(function(rawResult) {
      return _.get(rawResult, "[0].loc");
    });
};

var getUpToDateLocations = function(knex, slaves, locations) {
  return knex
    .select("t.location")
    .select(
      knex.raw(
        'max(pg_wal_lsn_diff("t"."location", "stats"."replay_lsn")) <= 0 as "is_up_to_date"'
      )
    )
    .from(
      knex.raw(
        "unnest(ARRAY[" +
          _.map(locations, _.constant("?::pg_lsn")).join(", ") +
          ']) as "t"("location")',
        locations
      )
    )
    .crossJoin(knex.raw('pg_stat_repl() as "stats"'))
    .whereIn("stats.client_addr", slaves)
    .groupBy("t.location");
};

var pgAsyncReplicationProto = {
  wait: function(timeout) {
    timeout = timeout || this.timeout;

    return new Promise(
      function(resolve, reject) {
        if (!this.slaves.length) return resolve();

        getMasterLocation(this.knex).then(
          function(masterLocation) {
            var operation = {
              location: masterLocation,
              resolve: function() {
                if (this.timeout) clearTimeout(this.timeout);
                resolve();
              },
              reject: reject
            };

            if (timeout) {
              operation.timeout = setTimeout(
                function() {
                  var idx = this.pending.indexOf(operation);
                  if (~idx) this.pending.splice(idx, 1);

                  reject(
                    new Error(
                      "pgAsyncReplication timed out waiting for database to be synced."
                    )
                  );
                }.bind(this),
                timeout
              );
            }

            this.pending.push(operation);
          }.bind(this)
        );
      }.bind(this)
    );
  },

  run: function(interval) {
    this.interval = setInterval(this.update.bind(this), interval);
  },

  stop: function() {
    if (this.interval) clearInterval(this.interval);
  },

  update: function() {
    if (!this.slaves.length || !this.pending.length) return;

    getUpToDateLocations(
      this.knex,
      this.slaves,
      _.pluck(this.pending, "location")
    ).then(
      function(results) {
        var locations = _.chain(results)
          .filter("is_up_to_date")
          .pluck("location")
          .value();

        _.remove(this.pending, function(operation) {
          if (!_.includes(locations, operation.location)) return false;

          operation.resolve();
          return true;
        });
      }.bind(this)
    );
  }
};

/**
 * Expose module.
 *
 * @param {Object} config Configuration object
 * @param {Number} config.interval Interval to pool slaves
 */

module.exports = _.curry(function(knex, config) {
  var pgAsyncReplication = Object.create(pgAsyncReplicationProto);

  _.merge(pgAsyncReplication, {
    pending: [],
    interval: null,
    knex: knex,
    slaves: config.slaves,
    timeout: config.timeout
  });

  pgAsyncReplication.run(config.interval);

  return pgAsyncReplication;
});
