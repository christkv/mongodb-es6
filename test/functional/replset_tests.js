"use strict"

var co = require('co');

exports['Should correctly connect to replicaset'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:31000/test', {}).connect()
      // Drop the collection
      try { yield client['tests']['cursors'].drop(); } catch(err){}
      // Execute multiple inserts
      var result = yield client['tests']['cursors'].insertMany([{a:2}, {a:3}]);

      // Iterate over the results
      var docs = yield client['tests']['cursors'].find({}).toArray();

      // Total number of docs available
      test.equal(2, docs.length);
      test.equal(2, docs[0].a);
      test.equal(3, docs[1].a);
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
