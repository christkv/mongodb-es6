"use strict"

let Collection = require('./collection')
  , CommandCursor = require('./command_cursor')
  , f = require('util').format;

// Illegal fields
let illegalFields = {
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
  constructor(name, topology, options) {
    this.name = name;
    this.topology = topology;
    this.options = options;
  }

  command(cmd) {
    let self = this;

    // Return a promise
    return new this.options.promise(function(resolve, reject) {
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
    return new this.options.promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.name), {dropDatabase:1}, function(err, r) {
        if(err) reject(err);
        else resolve(r);
      });
    });    
  }

  listCollections(filter) {
    filter = filter || {};
    // Set the CommandCursor constructor
    let options = { cursorFactory: CommandCursor, promise: this.options.promise};
    // Build the command
    let command = { listCollections : this.name, filter: filter};
    // Return an aggregation cursor    
    return this.topology.cursor(f('%s.$cmd', this.name), command, options);
  }

  collection(name, options) {
    return new Collection(this.name, name, this.topology, options);
  }
}

module.exports = Db;
module.exports.illegalFields = illegalFields;