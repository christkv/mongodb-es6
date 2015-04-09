"use strict"

var Cursor = require('./cursor')
  , CoreReadPreference = require('mongodb-core').ReadPreference
  , MongoError = require('mongodb-core').MongoError
  , inherits = require('util').inherits
  , Readable = require('stream').Readable
  , f = require('util').format;

class CommandCursor extends Cursor {
  constructor(bson, ns, cmd, options, topology, topologyOptions) {
    super(bson, ns, cmd, options, topology, topologyOptions);
  }

  /**
   * Set the batch size for the cursor.
   * @method
   * @param {number} value The batchSize for the cursor.
   * @throws {MongoError}
   * @return {CommandCursor}
   */
  batchSize(value) {
    if(this.state == 'closed' || this.isDead()) throw new MongoError("Cursor is closed");
    if(typeof value != 'number') throw new MongoError("batchSize requires an integer");
    if(this.cmd.cursor) this.cmd.cursor.batchSize = value;
    this.setCursorBatchSize(value);
    return this;
  }  

  get namespace() {
    if(!this) return null;
    var ns = this.ns || '';
    var firstDot = ns.indexOf('.');
    if (firstDot < 0) {
      return {
        database: this.ns,
        collection: ''
      };
    }
    return {
      database: ns.substr(0, firstDot),
      collection: ns.substr(firstDot + 1)
    };    
  }
}

module.exports = CommandCursor;