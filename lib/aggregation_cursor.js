"use strict"

var Cursor = require('./cursor')
  , CoreReadPreference = require('mongodb-core').ReadPreference
  , MongoError = require('mongodb-core').MongoError
  , inherits = require('util').inherits
  , Readable = require('stream').Readable
  , f = require('util').format;

class AggregationCursor extends Cursor {
  constructor(bson, ns, cmd, options, topology, topologyOptions) {
    super(bson, ns, cmd, options, topology, topologyOptions);
  }

  /**
   * Set the batch size for the cursor.
   * @method
   * @param {number} value The batchSize for the cursor.
   * @throws {MongoError}
   * @return {AggregationCursor}
   */
  batchSize(value) {
    if(this.state == 'closed' || this.isDead()) throw new MongoError("Cursor is closed");
    if(typeof value != 'number') throw new MongoError("batchSize requires an integer");
    if(this.cmd.cursor) this.cmd.cursor.batchSize = value;
    this.setCursorBatchSize(value);
    return this;
  }  

  /**
   * Add a geoNear stage to the aggregation pipeline
   * @method
   * @param {object} document The geoNear stage document.
   * @return {AggregationCursor}
   */
  geoNear(document) {
    this.cmd.pipeline.push({$geoNear: document});
    return this;
  }

  /**
   * Add a group stage to the aggregation pipeline
   * @method
   * @param {object} document The group stage document.
   * @return {AggregationCursor}
   */
  group(document) {
    this.cmd.pipeline.push({$group: document});
    return this;
  }

  /**
   * Add a limit stage to the aggregation pipeline
   * @method
   * @param {number} value The state limit value.
   * @return {AggregationCursor}
   */
  limit(value) {
    this.cmd.pipeline.push({$limit: value});
    return this;
  }

  /**
   * Add a match stage to the aggregation pipeline
   * @method
   * @param {object} document The match stage document.
   * @return {AggregationCursor}
   */
  match(document) {
    this.cmd.pipeline.push({$match: document});
    return this;
  }

  /**
   * Add a maxTimeMS stage to the aggregation pipeline
   * @method
   * @param {number} value The state maxTimeMS value.
   * @return {AggregationCursor}
   */
  maxTimeMS(value) {
    if(this.topology.lastIsMaster().minWireVersion > 2) {
      this.cmd.maxTimeMS = value;
    }
    return this;
  }

  /**
   * Add a out stage to the aggregation pipeline
   * @method
   * @param {number} destination The destination name.
   * @return {AggregationCursor}
   */
  out(destination) {
    this.cmd.pipeline.push({$out: destination});
    return this;
  }

  /**
   * Add a project stage to the aggregation pipeline
   * @method
   * @param {object} document The project stage document.
   * @return {AggregationCursor}
   */
  project(document) {
    this.cmd.pipeline.push({$project: document});
    return this;
  }

  /**
   * Add a redact stage to the aggregation pipeline
   * @method
   * @param {object} document The redact stage document.
   * @return {AggregationCursor}
   */
  redact(document) {
    this.cmd.pipeline.push({$redact: document});
    return this;
  }

  /**
   * Add a skip stage to the aggregation pipeline
   * @method
   * @param {number} value The state skip value.
   * @return {AggregationCursor}
   */
  skip(value) {
    this.cmd.pipeline.push({$skip: value});
    return this;
  }

  /**
   * Add a sort stage to the aggregation pipeline
   * @method
   * @param {object} document The sort stage document.
   * @return {AggregationCursor}
   */
  sort(document) {
    this.cmd.pipeline.push({$sort: document});
    return this;
  }

  /**
   * Add a unwind stage to the aggregation pipeline
   * @method
   * @param {number} field The unwind field name.
   * @return {AggregationCursor}
   */
  unwind(field) {
    this.cmd.pipeline.push({$unwind: field});
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

module.exports = AggregationCursor;