"use strict";

var Runner = require('integra').Runner
  , Cover = require('integra').Cover
  , RCover = require('integra').RCover
  , FileFilter = require('integra').FileFilter
  , NodeVersionFilter = require('./filters/node_version_filter')
  , MongoDBVersionFilter = require('./filters/mongodb_version_filter')
  , MongoDBTopologyFilter = require('./filters/mongodb_topology_filter')
  , OSFilter = require('./filters/os_filter')
  , TravisFilter = require('./filters/travis_filter')
  , DisabledFilter = require('./filters/disabled_filter')
  , FileFilter = require('integra').FileFilter
  , TestNameFilter = require('integra').TestNameFilter
  , path = require('path')
  , rimraf = require('rimraf')
  , fs = require('fs')
  , m = require('mongodb-version-manager')
  , f = require('util').format;

// console.log(argv._);
var argv = require('optimist')
    .usage('Usage: $0 -n [name] -f [filename]')
    .argv;

var shallowClone = function(obj) {
  var copy = {};
  for(var name in obj) copy[name] = obj[name];
  return copy;
}

// Skipping parameters
var startupOptions = {
    skipStartup: false
  , skipRestart: false
  , skipShutdown: false
  , skip: false
}

/**
 * Standalone MongoDB Configuration
 */
var createConfiguration = function(options) {
  options = options || {};

  // Create the configuration
  var Configuration = function(context) {
    var mongo = require('mongodb');
    var Db = mongo.Db;
    var Server = mongo.Server;
    // var Logger = mongo.Logger;
    var ServerManager = require('mongodb-tools').ServerManager;
    var database = "integration_tests";
    var url = options.url || "mongodb://%slocalhost:27017/" + database;
    var port = options.port || 27017;
    var host = options.host || 'localhost';
    var replicasetName = options.replicasetName || 'rs';
    var writeConcern = options.writeConcern || {w:1};
    var writeConcernMax = options.writeConcernMax || {w:1};

    // Logger.setCurrentLogger(function() {});
    // Logger.setLevel('debug');

    // Shallow clone the options
    var fOptions = shallowClone(options);
    options.journal = false;

    // Override manager or use default
    var manager = options.manager ? options.manager() : new ServerManager(fOptions);

    // clone
    var clone = function(o) {
      var p = {}; for(var name in o) p[name] = o[name];
      return p;
    }

    // return configuration
    return {
      manager: manager,
      replicasetName: replicasetName,

      start: function(callback) {
        if(startupOptions.skipStartup) return callback();
        manager.start({purge:true, signal:-9, kill:true}, function(err) {
          if(err) throw err;
          callback();
        });
      },

      stop: function(callback) {
        if(startupOptions.skipShutdown) return callback();
        manager.stop({signal: -15}, function() {
          callback();
        });
      },

      restart: function(options, callback) {
        if(typeof options == 'function') callback = options, options = {};
        if(startupOptions.skipRestart) return callback();
        var purge = typeof options.purge == 'boolean' ? options.purge : true;
        var kill = typeof options.kill == 'boolean' ? options.kill : true;
        manager.restart({purge:purge, kill:kill}, function() {
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

      newDbInstance: function(dbOptions, serverOptions) {
        serverOptions = serverOptions || {};
        // Override implementation
        if(options.newDbInstance) return options.newDbInstance(dbOptions, serverOptions);

        // Set up the options
        var keys = Object.keys(options);
        if(keys.indexOf('sslOnNormalPorts') != -1) serverOptions.ssl = true;

        // Fall back
        var port = serverOptions && serverOptions.port || options.port || 27017;
        var host = serverOptions && serverOptions.host || 'localhost';

        // Default topology
        var topology = Server;
        // If we have a specific topology
        if(options.topology) {
          topology = options.topology;
        }

        // Return a new db instance
        return new Db(database, new topology(host, port, serverOptions), dbOptions);
      },

      newDbInstanceWithDomainSocket: function(dbOptions, serverOptions) {
        // Override implementation
        if(options.newDbInstanceWithDomainSocket) return options.newDbInstanceWithDomainSocket(dbOptions, serverOptions);

        // Default topology
        var topology = Server;
        // If we have a specific topology
        if(options.topology) {
          topology = options.topology;
        }

        // Fall back
        var host = serverOptions && serverOptions.host || "/tmp/mongodb-27017.sock";

        // Set up the options
        var keys = Object.keys(options);
        if(keys.indexOf('sslOnNormalPorts') != -1) serverOptions.ssl = true;
        // If we explicitly testing undefined port behavior
        if(serverOptions && serverOptions.port == 'undefined') {
          return new Db('integration_tests', topology(host, undefined, serverOptions), dbOptions);
        }

        // Normal socket connection
        return new Db('integration_tests', topology(host, serverOptions), dbOptions);
      },

      url: function(username, password) {
        // Fall back
        var auth = "";

        if(username && password) {
          auth = f("%s:%s@", username, password);
        }

        return f(url, auth);
      },

      // Additional parameters needed
      require: mongo,
      database: database || options.database,
      nativeParser: true,
      port: port,
      host: host,
      writeConcern: function() { return clone(writeConcern) },
      writeConcernMax: function() { return clone(writeConcernMax) }
    }
  }

  return Configuration;
}

// Set up the runner
var runner = new Runner({
    // logLevel:'debug'
  runners: 1
  , failFast: true
});

var testFiles =[
  '/test/functional/mongoclient_tests.js'
]

// Add all the tests to run
testFiles.forEach(function(t) {
  if(t != "") runner.add(t);
});

// Add a Node version plugin
runner.plugin(new NodeVersionFilter(startupOptions));
// Add a MongoDB version plugin
runner.plugin(new MongoDBVersionFilter(startupOptions));
// Add a Topology filter plugin
runner.plugin(new MongoDBTopologyFilter(startupOptions));
// Add a OS filter plugin
runner.plugin(new OSFilter(startupOptions))
// Add a Disable filter plugin
runner.plugin(new DisabledFilter(startupOptions))

// Exit when done
runner.on('exit', function(errors, results) {
  process.exit(0)
});

// Are we running a functional test
var config = createConfiguration();

  // startupOptions.skipStartup = true;
  // startupOptions.skipRestart = true;
  // startupOptions.skipShutdown = true;
  // startupOptions.skip = true;

// If we have a test we are filtering by
if(argv.f) {
  runner.plugin(new FileFilter(argv.f));
}

if(argv.n) {
  runner.plugin(new TestNameFilter(argv.n));
}

// Add travis filter
runner.plugin(new TravisFilter());

// Remove db directories
try {
  rimraf.sync('./data');
  rimraf.sync('./db');
} catch(err) {
}

// Kill any running MongoDB processes and
// `install $MONGODB_VERSION` || `use existing installation` || `install stable`
m(function(err){
  if(err) return console.error(err) && process.exit(1);

  m.current(function(err, version){
    if(err) return console.error(err) && process.exit(1);
    console.log('Running tests against MongoDB version `%s`', version);
    // Run the configuration
    runner.run(config);
  });
});
