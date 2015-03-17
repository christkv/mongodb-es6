"use strict"

// Get the MongoClient
var Server = require('mongodb-core').Server
  , Db = require('./db')
  , Collection = require('./collection')
  , parser = require('./url_parser')
  , Proxy = require('harmony-proxy');

/*
 * Create a new topology object
 */
var createTopology = function(url, options) {
  if(url.servers.length == 1) {
    return new Server({
        host: url.servers[0].host
      , port: url.servers[0].port
      , reconnect: false
      , emitError: true
    })
  }
}

// Illegal fields
var illegalFields = {
    then: true
  , close: true
  , toJSON: true
  , inspect: true
  , toString: true
  , command: true
  , topology: true
};

/*
 * MongoClient class
 */
class MongoClient {
  constructor(url, options) {
    // Parse the url into an object
    this.url = parser(url);
    // Save the options
    this.options = options;
    // Create the topology
    this.topology = createTopology(this.url, options);
  }

  close() {
    this.topology.destroy();
  }

  connect() {
    var self = this;

    // Create connection promise
    var promise = new Promise(function(resolve, reject) {
      // Start the topology
      self.topology.on('connect', function(topology) {
        // Db instance
        var db = new Db('test', topology);
        // Wrap client in a proxy        
        var proxy = new Proxy(db, {
          get: function(target, name) {
            // Reserved fields
            if(MongoClient.illegalFields[name]) return target[name];

            // Create a new Db object  with the passed in name
            var newDb = new Db(name, topology);

            // Return the db in a proxy allowing us to do
            // client['test']['data'].insert()
            return new Proxy(newDb, {
              get: function(target, name) {
                // Reserved fields
                if(Db.illegalFields[name]) return target[name];
                // Return a collection
                return newDb.collection(name);
              }
            });
          }
        });

        // Return the proxy
        resolve(proxy);
      });

      // Catch any connection error
      self.topology.on('error', function(err) {
        reject(err);
      });

      // Connect the topology
      self.topology.connect();
    });
    
    // Return the promise
    return promise;
  }
}

module.exports = MongoClient;
module.exports.illegalFields = illegalFields;
