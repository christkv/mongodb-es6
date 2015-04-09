"use strict"

var co = require('co');

exports['Should correctly exercise aggregation cursor'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()

      // Drop the collection
      try { yield client['tests']['cursors'].drop(); } catch(err){}
      var result = yield client['tests']['cursors'].insertMany([{a:1}, {a:1}, {a:2}, {a:1}]);
      test.equal(4, result.result.n);

      // Iterate over the results
      var cursor = client['tests']['cursors']
        .aggregate([{$match: {}}], {
            allowDiskUse: true
          , batchSize: 2
          , maxTimeMS: 50
        });

      // Exercise all the options
      cursor = cursor.geoNear({geo:1})
          .group({group:1})
          .limit(10)
          .match({match:1})
          .maxTimeMS(10)
          .project({project:1})
          .redact({redact:1})
          .skip(1)
          .sort({sort:1})
          .batchSize(10)
          .unwind("name")
          .out("collection")
          
      // Should fail due to illegal options
      try {
        var result = cursor.toArray();
      } catch(err) {
        test.ok(err != null);        
      }

      // Drop the collection
      var cursor = client['tests']['cursors'].aggregate();
      var docs = yield cursor.match({a:1}).toArray();
      test.equal(3, docs.length);

      // Finish up
      client.close();
      test.done();
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}

exports['Should correctly stream aggregation cursor'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()
      // Drop the collection
      try { yield client['tests']['cursors'].drop(); } catch(err){}
      // Execute multiple inserts
      var result = yield client['tests']['cursors'].insertMany([{a:1}, {a:1}, {a:2}, {a:1}]);
      test.equal(4, result.result.n);

      // Set the cursor
      var cursor = client['tests']['cursors'].aggregate();
      var docs = [];

      // var docs = [];
      cursor.on('data', function(doc) {
        docs.push(doc);
      });

      cursor.on('end', function() {
        test.equal(4, docs.length);

        client.close();
        test.done();
      });
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}