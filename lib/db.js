"use strict"

var Collection = require('./collection')
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

  collection(name) {
    return new Collection(name, this.topology);
  }
}

module.exports = Db;
module.exports.illegalFields = illegalFields;