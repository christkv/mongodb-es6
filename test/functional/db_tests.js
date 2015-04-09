"use strict";

var co = require('co');

exports['Should correctly connect to MongoDB using MongoClient ES6 promise'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()
      // Execute command
      var result = yield client['admin'].command({ismaster:true});
      // Finish up
      test.ok(result != null);
      client.close();
      test.done();
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}

exports['Should correctly execute insert against multiple databases'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()
      // Execute single insert
      var result = yield client['tests']['documents'].insertOne({a:1});
      // Execute multiple inserts
      var result = yield client['tests']['documents'].insertMany([{a:2}, {a:3}]);
      // Finish up
      test.ok(result != null);
      client.close();
      test.done();
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}
