"use strict";

var mongodb = require('../../.')
  , co = require('co')
  , Db = mongodb.Db
  , Server = mongodb.Server
  , MongoClient = mongodb.MongoClient;

var simple_100_document_toArray = function(connection_string) {
  return function() {
    return {
      db: null,

      // Setup function, called once before tests are run
      setup: function(callback) {
        var self = this;

        co(function*() {
          self.db = yield new MongoClient(connection_string).connect();
          self.collection = self.db['tests']['simple_100_document_toArray'];

          try { self.collection.drop(); } catch(err) {};

          // Create 100 documents
          var docs = [];
          for(var i = 0; i < 100; i++) docs.push({a:1, b:'hello world', c:1});

          // Setup the 100 documents
          yield self.collection.insertMany(docs);
          callback();
        }).catch(function(err) {
          console.dir(err)
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
          yield self.collection.find({}).toArray();
          callback();
        }).catch(function(err) {
          console.dir(err)
        });
      }
    }
  }
}

var simple_2_document_limit_toArray = function(connection_string) {
  return function() {
    return {
      db: null,

      // Setup function, called once before tests are run
      setup: function(callback) {
        var self = this;

        co(function*() {
          self.db = yield new MongoClient(connection_string).connect();
          self.collection = self.db['tests']['simple_2_document_limit_toArray'];

          try { self.collection.drop(); } catch(err) {};

          // Create 100 documents
          var docs = [];
          for(var i = 0; i < 1000; i++) docs.push({a:1, b:'hello world', c:1});

          // Setup the 100 documents
          yield self.collection.insertMany(docs);
          callback();
        }).catch(function(err) {
          console.dir(err)
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
          yield self.collection.find({}).limit(2).toArray();
          callback();
        }).catch(function(err) {
          console.dir(err)
        });
      }
    }
  }
}

exports.simple_100_document_toArray = simple_100_document_toArray;
exports.simple_2_document_limit_toArray = simple_2_document_limit_toArray;