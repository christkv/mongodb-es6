exports['Should correctly connect to MongoDB using MongoClient ES6 promise'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    var client = new MongoClient(configuration.url(), {});
    var promise = client.connect();

    // Add ourselves to promise chain
    promise.then(function(client) {
      test.ok(client != null);
      client.close();
      test.done();
    });
  }
}

exports['Should fail to connect to MongoDB using MongoClient ES6 promise'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    var client = new MongoClient('mongodb://localhost:28000/test', {});
    var promise = client.connect();

    // Add ourselves to promise chain
    promise.catch(function(err) {
      test.ok(err != null);
      test.done();
    }).then(function(client) {
    });
  }
}
