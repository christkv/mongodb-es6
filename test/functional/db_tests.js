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
      // Drop the database
      var result = yield client['tests'].drop();
      test.equal('tests', result.result.dropped);
      // Execute single insert
      var result = yield client['tests']['documents'].insertOne({a:1});
      // Execute multiple inserts
      var result = yield client['tests']['documents'].insertMany([{a:2}, {a:3}]);
      // List the collections
      var collections = yield client['tests'].listCollections().toArray();
      test.equal(2, collections.length);
      // List the collections
      var collections = yield client['tests'].listCollections({name: 'documents'}).toArray();
      test.equal(1, collections.length);
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
