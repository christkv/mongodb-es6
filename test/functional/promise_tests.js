"use strict";

var co = require('co');

exports['Should correctly insert documents using Bluebird promise library'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient
      , P = require("bluebird");

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test').promiseLibrary(P).connect();
      // Drop the collection
      try { 
        yield client['tests']['documents'].drop(); 
        yield client['tests']['documents1'].drop(); 
      } catch(err){}
      // Execute single insert
      var result = yield client['tests']['documents'].insertOne({a:1});
      test.ok(client['tests']['documents'].insertOne({a:1}) instanceof P);
      // Execute multiple inserts
      var result = yield client['tests']['documents1'].insertMany([{a:2}, {a:3}]);
      // Query the data
      var docs = yield client['tests']['documents'].find({}).toArray()
      var docs1 = yield client['tests']['documents1'].find({}).toArray()
      test.ok(client['tests']['documents'].find({}).toArray() instanceof P);

      // Finish up
      test.ok(1, docs.length);
      test.ok(2, docs1.length);
      client.close();
      test.done();
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}
