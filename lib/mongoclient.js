"use strict"

// Get the MongoClient
let Server = require('mongodb-core').Server
  , Db = require('./db')
  , Collection = require('./collection')
  , parser = require('./url_parser')
  , Proxy = require('harmony-proxy');

/*
 * Create a new topology object
 */
let createTopology = function(url, options) {
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
let illegalFields = {
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
    let self = this;

    // Create connection promise
    let promise = new Promise(function(resolve, reject) {
      // Start the topology
      self.topology.on('connect', function(topology) {
        // Db instance
        let db = new Db('test', topology);
        // Wrap client in a proxy        
        let proxy = new Proxy(db, {
          get: function(target, dbName) {
            // Reserved fields
            if(MongoClient.illegalFields[dbName]) return target[dbName];

            // Create a new Db object  with the passed in name
            let newDb = new Db(dbName, topology);

            // Return the db in a proxy allowing us to do
            return new Proxy(newDb, {
              get: function(target, collectionName) {
                // Reserved fields
                if(Db.illegalFields[collectionName]) return target[collectionName];
                // Return a collection
                return newDb.collection(collectionName);
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
