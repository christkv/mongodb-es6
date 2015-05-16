"use strict"

var co = require('co');

exports['Should correctly execute toArray on cursor'] = {
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

exports['Should correctly exercise all methods on cursor before toArray'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient
      , ReadPreference = require('../..').ReadPreference;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()
      // Drop the collection
      try { yield client['tests']['cursors'].drop(); } catch(err){}

      // Build an array of items
      var docs = [];
      for(var i = 0; i < 100; i++) docs.push({a:i, b:i, c:i});

      // Execute multiple inserts
      var result = yield client['tests']['cursors'].insertMany(docs);
      test.ok(result != null);

      // Iterate over the results
      var docs = yield client['tests']['cursors']
        .find({})
        // Change the query
        .filter({a: {$gte: 10}})
        // Set the addCursorFlag
        .addCursorFlag('noCursorTimeout', true)
        // Set the maxTimeMS
        .maxTimeMS(1000)
        // Set the comment
        .comment('Execute the test comment')
        // Set the sort
        .sort({a:1}, -1)
        // Query modifier
        .addQueryModifier('$orderby', {a:1})
        // Set a read preference
        .setReadPreference(new ReadPreference('primary'))
        // Project
        .project({a:1, b:1})
        // Set the limit
        .limit(20)
        // Set the skip of the query
        .skip(10)
        // Set the batch size
        .batchSize(10)
        // Map all the values
        .map(function(x) { x.d = 1; return x; })
        // Get all documents
        .toArray();

      // Total number of docs available
      test.equal(20, docs.length);
      test.equal(20, docs[0].a);
      test.equal(20, docs[0].b);
      test.equal(undefined, docs[0].c);
      test.equal(1, docs[0].d);
      client.close();
      test.done();
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}

exports['Should correctly exercise all methods on cursor before explain'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient
      , ReadPreference = require('../..').ReadPreference;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()
      // Drop the collection
      try { yield client['tests']['cursors'].drop(); } catch(err){}

      // Build an array of items
      var docs = [];
      for(var i = 0; i < 100; i++) docs.push({a:i, b:i, c:i});

      // Execute multiple inserts
      var result = yield client['tests']['cursors'].insertMany(docs);
      test.ok(result != null);

      // Iterate over the results
      var doc = yield client['tests']['cursors']
        .find({})
        // Change the query
        .filter({a: {$gte: 10}})
        // Set the addCursorFlag
        .addCursorFlag('noCursorTimeout', true)
        // Set the maxTimeMS
        .maxTimeMS(1000)
        // Set the comment
        .comment('Execute the test comment')
        // Set the sort
        .sort({a:1}, -1)
        // Query modifier
        .addQueryModifier('$orderby', {a:1})
        // Set a read preference
        .setReadPreference(new ReadPreference('primary'))
        // Project
        .project({a:1, b:1})
        // Set the limit
        .limit(20)
        // Set the skip of the query
        .skip(10)
        // Set the batch size
        .batchSize(10)
        // Map all the values
        .map(function(x) { x.d = 1; return x; })
        // Get all documents
        .explain();

      // Total number of docs available
      test.ok(doc != null);
      test.equal('tests', client['tests']['cursors'].find({}).namespace.database);
      test.equal('cursors', client['tests']['cursors'].find({}).namespace.collection);
      client.close();
      test.done();
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}

exports['Should correctly exercise all methods on cursor before count'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient
      , ReadPreference = require('../..').ReadPreference;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()
      // Drop the collection
      try { yield client['tests']['cursors'].drop(); } catch(err){}

      // Build an array of items
      var docs = [];
      for(var i = 0; i < 100; i++) docs.push({a:i, b:i, c:i});

      // Execute multiple inserts
      var result = yield client['tests']['cursors'].insertMany(docs);
      test.ok(result != null);

      // Iterate over the results
      var cursor = client['tests']['cursors']
        .find({})
        // Change the query
        .filter({a: {$gte: 10}})
        // Set the addCursorFlag
        .addCursorFlag('noCursorTimeout', true)
        // Set the maxTimeMS
        .maxTimeMS(1000)
        // Set the comment
        .comment('Execute the test comment')
        // Set the sort
        .sort({a:1}, -1)
        // Query modifier
        .addQueryModifier('$orderby', {a:1})
        // Set a read preference
        .setReadPreference(new ReadPreference('primary'))
        // Project
        .project({a:1, b:1})
        // Set the limit
        .limit(20)
        // Set the skip of the query
        .skip(10)
        // Set the batch size
        .batchSize(10)
        // Set the preferred index hint
        .hint('a_1')
        // Map all the values
        .map(function(x) { x.d = 1; return x; });
      
      // Get all documents
      var doc = yield cursor.count();
      // Total number of docs available
      test.equal(20, doc);
      client.close();
      test.done();
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}

exports['Should correctly iterate over cursor'] = {
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
      var result = yield client['tests']['cursors'].insertMany([{a:2}, {a:3}, {a:4}, {a:5}]);

      // Set the cursor
      var cursor = client['tests']['cursors'].find({});
      var docs = [];

      // Return
      while(yield cursor.hasNext()) {
        let doc = yield cursor.next();
        docs.push(doc);
      }

      // Total number of docs available
      test.equal(4, docs.length);
      test.equal(2, docs[0].a);
      test.equal(3, docs[1].a);

      // Set the cursor
      var cursor = client['tests']['cursors'].find({}).batchSize(2);
      var docs = [];

      // Hold the cursor result
      let doc = null;
      // Iterate using the next function only
      while((doc = yield cursor.next()) != null) {
        docs.push(doc);
      }

      // Total number of docs available
      test.equal(4, docs.length);
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

exports['Should correctly stream cursor'] = {
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
      var result = yield client['tests']['cursors'].insertMany([{a:2}, {a:3}]);

      // Set the cursor
      var cursor = client['tests']['cursors'].find({});
      var docs = [];

      // var docs = [];
      cursor.on('data', function(doc) {
        docs.push(doc);
      });

      cursor.on('end', function() {
        test.equal(2, docs.length);

        client.close();
        test.done();
      });
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}