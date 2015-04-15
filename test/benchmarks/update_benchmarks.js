"use strict";

var mongodb = require('../../.')
  , Db = mongodb.Db
  , co = require('co')
  , Server = mongodb.Server
  , Binary = mongodb.Binary
  , MongoClient = mongodb.MongoClient;

var single_doc_upsert = function(connection_string) {
  return function() {
    return {
      db: null,
      i: 0,

      // Setup function, called once before tests are run
      setup: function(callback) {
        var self = this;

        co(function*() {
          self.db = yield new MongoClient(connection_string).connect();
          self.collection = self.db['tests']['single_doc_upsert'];
          callback();
        });
      },

      // Setup function, called once after test are run
      teardown: function(callback) {
        if(this.db != null) this.db.close();
        callback();
      },

      // Actual operation we are measuring
      test: function(callback) {
        var self = this;

        co(function*() {
          yield self.collection.updateOne({a:this.i++}, {$set: {b: 1}}, {upsert:true});
          callback();
        }).catch(function(err) {
          console.dir(err)
        });
      }
    }
  }
}

exports.single_doc_upsert = single_doc_upsert;
