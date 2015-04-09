"use strict";

var Runner = require('integra').Runner
  , Cover = require('integra').Cover
  , RCover = require('integra').RCover
  , f = require('util').format
  , path = require('path')
  , NodeVersionFilter = require('./filters/node_version_filter')
  , MongoDBVersionFilter = require('./filters/mongodb_version_filter')
  , MongoDBTopologyFilter = require('./filters/mongodb_topology_filter')
  , TravisFilter = require('./filters/travis_filter')
  , FileFilter = require('integra').FileFilter
  , TestNameFilter = require('integra').TestNameFilter
  , ServerManager = require('mongodb-tools').ServerManager
  , ReplSetManager = require('mongodb-tools').ReplSetManager
  , ShardingManager = require('mongodb-tools').ShardingManager;

var argv = require('optimist')
    .usage('Usage: $0 -t [target] -e [environment] -n [name] -f [filename] -r [smoke report file]')
    .demand(['t'])
    .argv;

/**
 * Standalone MongoDB Configuration
 */
var f = require('util').format;
var Logger = require('mongodb-core').Logger;

var Configuration = function(options) {
  options = options || {};
  var host = options.host || 'localhost';
  var port = options.port || 27017;
  var db = options.db || 'integration_tests';
  var mongo = null;
  var manager = options.manager;
  var skipStart = typeof options.skipStart == 'boolean' ? options.skipStart : false;
  var skipTermination = typeof options.skipTermination == 'boolean' ? options.skipTermination : false;
  var setName = options.setName || 'rs';

  // Default function
  var defaultFunction = function(self, _mongo) {
    return new _mongo.Server({
        host: self.host
      , port: self.port
    });
  };

  // Create a topology function
  var topology = options.topology || defaultFunction;

  return function(context) {
    mongo = require('mongodb-core');

    return {
      start: function(callback) {
        var self = this;
        if(skipStart) return callback();
        // Start the db
        manager.start({purge:true, signal: -9}, function(err) {
          var server = topology(self, mongo);
          // Set up connect
          server.once('connect', function() {
            // Drop the database
            server.command(f("%s.$cmd", self.db), {dropDatabase: 1}, function(err, r) {
              server.destroy();
              callback();
            });
          });

          // Connect
          server.connect();
        });
      },

      stop: function(callback) {
        if(skipTermination) return callback();
        manager.stop({signal: -15}, function() {
          callback();
        });
      },

      restart: function(options, callback) {
        if(typeof options == 'function') callback = options, options = {purge:true, kill:true};

        manager.restart(options, function() {
          setTimeout(function() {
            callback();
          }, 1000);
        });
      },

      setup: function(callback) {
        callback();
      },

      teardown: function(callback) {
        callback();
      },

      url: function() {
        return options.url || 'mongodb://localhost:27017/test';
      },

      newTopology: function(options, callback) {
        if(typeof options == 'function') {
          callback = options;
          options = {};
        }

        callback(null, topology(this, mongo));
      },

      newConnection: function(options, callback) {
        if(typeof options == 'function') {
          callback = options;
          options = {};
        }

        var server = topology(this, mongo);
        // Set up connect
        server.once('connect', function() {
          callback(null, server);
        });

        // Connect
        server.connect();
      },

      // Additional parameters needed
      require: mongo,
      port: port,
      host: host,
      setName: setName,
      db: db,
      manager: manager,
      writeConcern: function() { return {w: 1} }
    }
  }
}

// Set up the runner
var runner = new Runner({
    logLevel:'debug'
  , runners: 1
  , failFast: true
});

var testFiles =[
    '/test/functional/db_tests.js'
  , '/test/functional/collection_tests.js'
  , '/test/functional/mongoclient_tests.js'
  , '/test/functional/cursor_tests.js'
  , '/test/functional/crud_tests.js'
]

// Add all the tests to run
testFiles.forEach(function(t) {
  if(t != "") runner.add(t);
});

// Add a Node version plugin
runner.plugin(new NodeVersionFilter());
// Add a MongoDB version plugin
runner.plugin(new MongoDBVersionFilter());
// Add a Topology filter plugin
runner.plugin(new MongoDBTopologyFilter());

// Exit when done
runner.on('exit', function(errors, results) {
  process.exit(0)
});

// // Set Logger level for driver
// Logger.setLevel('error');
// Logger.filter('class', ['ReplSet']);

// If we have a test we are filtering by
if(argv.f) {
  runner.plugin(new FileFilter(argv.f));
}

if(argv.n) {
  runner.plugin(new TestNameFilter(argv.n));
}

// Add travis filter
runner.plugin(new TravisFilter());

//
// Single server
var config = {
    host: 'localhost'
  , port: 27017
  // , skipStart: false
  // , skipTermination: false
  , skipStart: true
  , skipTermination: true
  , manager: new ServerManager({
      dbpath: path.join(path.resolve('db'), f("data-%d", 27017))
    , logpath: path.join(path.resolve('db'), f("data-%d.log", 27017))
  })
}

// Run the configuration
runner.run(Configuration(config));
