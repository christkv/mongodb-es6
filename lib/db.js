"use strict"

var Collection = require('./collection')
  , CommandCursor = require('./command_cursor')
  , f = require('util').format;

// Illegal fields
var illegalFields = {
    then: true
  , close: true
  , toJSON: true
  , inspect: true
  , toString: true
  , client: true
  , command: true
  , db: true
  , topology: true
  , name: true
  , drop: true
  , listCollections: true
}

class Db {
  constructor(name, topology) {
    this.name = name;
    this.topology = topology;
  }

  command(cmd) {
    var self = this;

    // Return a promise
    return new Promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.name), cmd, function(err, r) {
        if(err) reject(err);
        else resolve(r);
      });
    });
  }

  close() {
    this.topology.destroy();
  }

  drop() {
    let self = this;

    // Return a promise
    return new Promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.name), {dropDatabase:1}, function(err, r) {
        if(err) reject(err);
        else resolve(r);
      });
    });    
  }

  listCollections(filter) {
    filter = filter || {};
    // Set the CommandCursor constructor
    var options = { cursorFactory: CommandCursor };
    // Build the command
    var command = { listCollections : this.name, filter: filter};
    // Return an aggregation cursor    
    return this.topology.cursor(f('%s.$cmd', this.name), command, options);
  }

  collection(name) {
    return new Collection(this.name, name, this.topology);
  }
}

module.exports = Db;
module.exports.illegalFields = illegalFields;