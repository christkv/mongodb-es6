"use strict"

// Get the MongoClient
let Server = require('mongodb-core').Server
  , ReplSet = require('mongodb-core').ReplSet
  , Mongos = require('mongodb-core').Mongos
  , Db = require('./db')
  , Collection = require('./collection')
  , parser = require('./url_parser')
  , Proxy = require('harmony-proxy')
  , shallowClone = require('./utils').shallowClone
  , merge = require('./utils').merge;

let createReplSet = function(ismaster, options, callback) {
  var servers = ismaster.hosts.map(function(x) {
    var parts = x.split(":");
    return {host: parts[0], port: parseInt(parts[1])};
  });

  callback(null, new ReplSet(servers, options));
}

let createMongos = function(ismaster, options, callback) {
  callback(null, new Mongos(options.servers, options));
}

let createServer = function(ismaster, options, callback) {
  var serverOptions = merge(options.servers[0], options);
  callback(null, new Server(serverOptions));
}

let identify = function(options, callback) {
  // Clone the options and merge
  var serverOptions = merge(options.servers[0], options);
  // No reconnect and emit error
  serverOptions.reconnect = false;
  serverOptions.emitError = true;
  serverOptions.connectionTimeout = typeof serverOptions.connectionTimeout == 'number' 
    ? serverOptions.connectionTimeout : 10000;

  // Attempt to connect
  var topology = new Server(serverOptions);
  topology.once('connect', function(topology) {
    // Get the ismaster
    var ismaster = topology.lastIsMaster()
    
    // Destroy the topology
    topology.destroy();

    // Create the correct topology
    if(ismaster.setName) 
      return createReplSet(ismaster, options, callback);
    else if(ismaster.msg == 'isdbgrid')
      return createMongos(ismaster, options, callback);
    else
      return createServer(ismaster, options, callback);
  });

  topology.once('error', function(err) {
    callback(err);
  });

  topology.connect();
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

/**
 * Connect to MongoDB using a url as documented at
 *
 *  docs.mongodb.org/manual/reference/connection-string/
 *
 * All the options below map to their mongodb-core options for the topologies.
 *
 * @class
 * @param {string} url The connection URI string
 * @param {object} [options=null] Optional settings. 
 * @param {boolean} [options.ssl=false] Enable ssl connection
 * @param {boolean} [options.sslValidate=true] Validate mongod server certificate against ca.
 * @param {Buffer[]|string[]} [options.sslCA=null] Array of valid certificates either as Buffers or Strings.
 * @param {Buffer|string} [options.sslCert=null] String or buffer containing the certificate we wish to present.
 * @param {Buffer|string} [options.sslKey=null] String or buffer containing the certificate private key we wish to present.
 * @param {Buffer|string} [options.sslPass=null] String or buffer containing the certificate password.
 * @param {boolean} [options.reconnect=true] Server will attempt to reconnect on loss of connection
 * @param {number} [options.reconnectTries=30] Server attempt to reconnect #times
 * @param {number} [options.reconnectInterval=1000] Server will wait # milliseconds between retries
 * @param {number} [options.size=5] Server connection pool size
 * @param {boolean} [options.keepAlive=true] TCP Connection keep alive enabled
 * @param {number} [options.keepAliveInitialDelay=0] Initial delay before TCP keep alive enabled
 * @param {boolean} [options.noDelay=true] TCP Connection no delay
 * @param {number} [options.connectionTimeout=0] TCP Connection timeout setting
 * @param {number} [options.socketTimeout=0] TCP Socket timeout setting
 * @param {boolean} [options.secondaryOnlyConnectionAllowed=false] Allow connection to a secondary only replicaset
 * @param {number} [options.haInterval=5000] The High availability period for replicaset inquiry
 * @param {number} [options.pingInterval=5000] Ping interval to check the response time to the different servers
 * @param {number} [options.acceptableLatency=250] Acceptable latency for selecting a server for reading (in milliseconds)
 * @return {MongoClient}
 */  
class MongoClient {
  constructor(url, options) {
    // Parse the url into an object
    this.url = parser(url);
    // Save the options
    this.options = options;
    // Merged options
    this.merged = merge(this.url, this.options);
  }

  close() {
    this.topology.destroy();
  }

  connect() {
    let self = this;

    // Create connection promise
    let promise = new Promise(function(resolve, reject) {      
      // Identify the right topology
      identify(self.merged, function(err, topology) {
        if(err) return reject(err);
        // Save the topology
        self.topology = topology;

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
      })
    });
    
    // Return the promise
    return promise;
  }
}

module.exports = MongoClient;
module.exports.illegalFields = illegalFields;
