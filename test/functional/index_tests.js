"use strict";

var co = require('co');

exports['should correctly exercise the create index methods'] = {
  metadata: { requires: { } },
  
  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()

      try { yield client['tests']['index1'].drop(); } catch(err){}
      // Create an index
      var result = yield client['tests']['index1'].createIndex({a:1}, {unique:true});
      test.equal(2, result.result.numIndexesAfter);

      // List the indexes
      var docs = yield client['tests']['index1'].listIndexes().toArray();
      test.equal(2, docs.length);

      // Drop the index
      var result = yield client['tests']['index1'].dropIndex({a:1});
      test.equal(2, result.result.nIndexesWas);

      // Finish up
      client.close();
      test.done();
    });
  }
}

exports['should correctly exercise the create indexes methods'] = {
  metadata: { requires: { } },
  
  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()

      try { yield client['tests']['index1'].drop(); } catch(err){}
      // Create an index
      var result = yield client['tests']['index1'].createIndexes([
          { key: {a:1}, unique:true }
        , { key: {b:1} }
      ]);

      test.equal(3, result.result.numIndexesAfter);

      // List the indexes
      var docs = yield client['tests']['index1'].listIndexes().toArray();
      test.equal(3, docs.length);

      // Drop the indexes
      var result = yield client['tests']['index1'].dropIndexes();
      test.equal(3, result.result.nIndexesWas);

      // Finish up
      client.close();
      test.done();
    });
  }
}