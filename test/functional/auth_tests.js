"use strict"

var co = require('co');

// use admin
// db.createUser(
//    {
//      user: "a",
//      pwd: "a",
//      roles: [ "readWrite", "userAdminAnyDatabase", "root", "dbOwner" ]
//    }
// )

exports['Should correctly authenticate'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://a:a@localhost:27017/admin', {}).connect()

      // Drop the collection
      try { yield client['tests']['cursors'].drop(); } catch(err){}
      var result = yield client['tests']['cursors'].insertMany([{a:1}, {a:1}, {a:2}, {a:1}]);
      test.equal(4, result.result.n);

      // Finish up
      client.close();
      test.done();
    }).catch(function(err) {
      console.log(err.stack)
      test.done();
    });
  }
}
