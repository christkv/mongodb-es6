"use strict"

var f = require('util').format
  , Cursor = require('./cursor');

class Collection {
  constructor(dbName, collectionName, topology) {
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.ns = f('%s.%s', dbName, collectionName);
    this.topology = topology;
  }

  insertOne(doc, options) {
    return this.insertMany([doc], options);
  }

  insertMany(docs, options) {
    options = options != null ? options : {};
    let self = this;    

    return new Promise(function(resolve, reject) {
      self.topology.insert(self.ns, docs, options, function(err, r) {
        if(err) reject(err);
        else resolve(r);
      });
    });
  }

  drop() {
    var self = this;

    return new Promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.dbName), {drop: self.collectionName}, function(err, r) {
        if(err) return reject(err);
        resolve(r);
      });
    });
  }

  find(query) {
    // Build the find command
    var findCommand = {
        find: this.ns
      , query: query
    }

    // Set the cursor factory we are using
    var options = {cursorFactory: Cursor};
    // Return a command
    return this.topology.cursor(this.ns, findCommand, options);
  }
}

module.exports = Collection;