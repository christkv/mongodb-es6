"use strict";

var mongodb = require('../../.')
  , Db = mongodb.Db
  , co = require('co')
  , f = require('util').format
  , Server = mongodb.Server
  , Binary = mongodb.Binary
  , MongoClient = mongodb.MongoClient;

var single_doc_insert = function(connection_string) {
  return function() {
    return {
      db: null,

      // Setup function, called once before tests are run
      setup: function(callback) {
        var self = this;

        co(function*() {
          self.db = yield new MongoClient(connection_string).connect();
          self.collection = self.db['tests']['single_doc_insert'];
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
          yield self.collection.insertOne({a:1});
          callback();
        }).catch(function(err) {
          console.dir(err)
        });
      }
    }
  }
}

var single_doc_insert_journal = function(connection_string) {
  return function() {
    return {
      db: null,

      // Setup function, called once before tests are run
      setup: function(callback) {
        var self = this;
        connection_string = f('%s?j=true', connection_string);

        co(function*() {
          self.db = yield new MongoClient(connection_string).connect();
          self.collection = self.db['tests']['single_doc_insert'];
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
          var options = {writeConcern: {j:true}};
          options = {};
          yield self.collection.insertOne({a:1}, options);
          callback();
        }).catch(function(err) {
          console.dir(err)
        });
      }
    }
  }
}

var single_100_simple_insert = function(connection_string) {
  return function() {
    return {
      db: null,
      docs: [],

      // Setup function, called once before tests are run
      setup: function(callback) {
        var self = this;

        co(function*() {
          self.db = yield new MongoClient(connection_string).connect();
          self.collection = self.db['tests']['single_doc_insert'];

          for(var i = 0; i < 100; i++) {
            self.docs.push({a:1, b: i, string: 'hello world', bin: new Buffer(256)})
          }

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
          yield self.collection.insertMany(self.docs, {writeConcern: {j:true}});
          callback();
        }).catch(function(err) {
          console.dir(err)
        });
      }
    }
  }
}

exports.single_doc_insert = single_doc_insert;
exports.single_100_simple_insert = single_100_simple_insert;
exports.single_doc_insert_journal = single_doc_insert_journal;