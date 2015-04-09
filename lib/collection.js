"use strict"

var f = require('util').format
  , shallowClone = require('./utils').shallowClone
  , writeConcern = require('./bulk/common').writeConcern
  , ObjectId = require('mongodb-core').BSON.ObjectId
  , OrderedBulkOperation = require('./bulk/ordered')
  , UnorderedBulkOperation = require('./bulk/unordered')
  , Cursor = require('./cursor');

var decorateUpdateWriteResults = function(r) {
  r.matchedCount = r.result.n;
  r.modifiedCount = r.result.nModified != null ? r.result.nModified : r.result.n;
  r.upsertedId = Array.isArray(r.result.upserted) && r.result.upserted.length > 0 ? r.result.upserted[0] : null;
  r.upsertedCount = Array.isArray(r.result.upserted) && r.result.upserted.length ? r.result.upserted.length : 0;
  return r;
}

var mergeFindAndModifyOptions = function(command, options, names) {
  for(var n of names) {
    command[n] = options[n];
  }

  return command;
}

class Collection {
  constructor(dbName, collectionName, topology) {
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.ns = f('%s.%s', dbName, collectionName);
    this.topology = topology;
  }

  insertOne(doc, options) {
    options = options != null ? options : {};
    let self = this;
    if(doc._id == null) doc._id = new ObjectId();

    return new Promise(function(resolve, reject) {
      self.topology.insert(self.ns, [doc], options, function(err, r) {
        if(err) return reject(err);
        r.insertedCount = r.result.n;
        r.insertedId = doc._id;
        resolve(r);
      });
    });
  }

  insertMany(docs, options) {
    options = options != null ? options : {};
    let self = this;

    return new Promise(function(resolve, reject) {
      // Add _id if not specified
      for(var i = 0; i < docs.length; i++) {
        if(docs[i]._id == null) docs[i]._id = new ObjectId();
      }

      self.topology.insert(self.ns, docs, options, function(err, r) {
        if(err) return reject(err);
        r.insertedCount = r.result.n;
        var ids = [];
        for(var i = 0; i < docs.length; i++) {
          if(docs[i]._id) ids.push(docs[i]._id);
        }
        r.insertedIds = ids;
        resolve(r);
      });
    });
  }

  /**
   * Update a single document on MongoDB
   * @method
   * @param {object} filter The Filter used to select the document to update
   * @param {object} update The update operations to be applied to the document
   * @param {object} [options=null] Optional settings.
   * @param {boolean} [options.upsert=false] Update operation is an upsert.
   * @param {(number|string)} [options.w=null] The write concern.
   * @param {number} [options.wtimeout=null] The write concern timeout.
   * @param {boolean} [options.j=false] Specify a journal write concern.
   * @return {Promise}
   */
  updateOne(filter, update, options) {
    var self = this;
    options = options || {};
    options = shallowClone(options);

    return new Promise(function(resolve, reject) {
      self.topology.update(self.ns, [{
          q: filter, u: update
        , multi:false, upsert: typeof options.upsert == 'boolean' ? options.upsert : false
      }], options, function(err, r) {
        if(err) return reject(err);
        resolve(decorateUpdateWriteResults(r));
      });
    });
  }

  /**
   * Update multiple documents on MongoDB
   * @method
   * @param {object} filter The Filter used to select the document to update
   * @param {object} update The update operations to be applied to the document
   * @param {object} [options=null] Optional settings.
   * @param {boolean} [options.upsert=false] Update operation is an upsert.
   * @param {(number|string)} [options.w=null] The write concern.
   * @param {number} [options.wtimeout=null] The write concern timeout.
   * @param {boolean} [options.j=false] Specify a journal write concern.
   * @return {Promise}
   */
  updateMany(filter, update, options) {
    var self = this;
    options = options || {};
    options = shallowClone(options);

    return new Promise(function(resolve, reject) {
      self.topology.update(self.ns, [{
          q: filter, u: update
        , multi:true, upsert: typeof options.upsert == 'boolean' ? options.upsert : false
      }], options, function(err, r) {
        if(err) return reject(err);
        resolve(decorateUpdateWriteResults(r));
      });
    });
  }

  /**
   * Replace a document on MongoDB
   * @method
   * @param {object} filter The Filter used to select the document to update
   * @param {object} doc The Document that replaces the matching document
   * @param {object} [options=null] Optional settings.
   * @param {boolean} [options.upsert=false] Update operation is an upsert.
   * @param {(number|string)} [options.w=null] The write concern.
   * @param {number} [options.wtimeout=null] The write concern timeout.
   * @param {boolean} [options.j=false] Specify a journal write concern.
   * @return {Promise}
   */
  replaceOne(filter, update, options) {
    var self = this;
    options = options || {};
    options = shallowClone(options);
    return this.updateOne(filter, update, options);
  }

  /**
   * Delete a document on MongoDB
   * @method
   * @param {object} filter The Filter used to select the document to remove
   * @param {object} [options=null] Optional settings.
   * @param {(number|string)} [options.w=null] The write concern.
   * @param {number} [options.wtimeout=null] The write concern timeout.
   * @param {boolean} [options.j=false] Specify a journal write concern.
   * @return {Promise}
   */
  deleteOne(filter, options) {
    var self = this;
    options = options || {};
    options = shallowClone(options);

    return new Promise(function(resolve, reject) {
      self.topology.remove(self.ns, [{
        q: filter, limit:1
      }], options, function(err, r) {
        if(err) return reject(err);
        r.deletedCount = r.result.n;        
        resolve(r);
      });
    });
  }

  /**
   * Delete multiple documents on MongoDB
   * @method
   * @param {object} filter The Filter used to select the documents to remove
   * @param {object} [options=null] Optional settings.
   * @param {(number|string)} [options.w=null] The write concern.
   * @param {number} [options.wtimeout=null] The write concern timeout.
   * @param {boolean} [options.j=false] Specify a journal write concern.
   * @return {Promise}
   */
  deleteMany(filter, options) {
    var self = this;
    options = options || {};
    options = shallowClone(options);

    return new Promise(function(resolve, reject) {
      self.topology.remove(self.ns, [{
        q: filter, limit:0
      }], options, function(err, r) {
        if(err) return reject(err);
        r.deletedCount = r.result.n;        
        resolve(r);
      });
    });
  }

  /**
   * Find a document and delete it in one atomic operation, requires a write lock for the duration of the operation.
   *
   * @method
   * @param {object} filter Document selection filter.
   * @param {object} [options=null] Optional settings.
   * @param {object} [options.projection=null] Limits the fields to return for all matching documents.
   * @param {object} [options.sort=null] Determines which document the operation modifies if the query selects multiple documents.
   * @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
   * @return {Promise}
   */
  findOneAndDelete(filter, options) {
    var self = this;
    options = options || {};
    options = shallowClone(options);

    var command = {
      query: filter, remove: true
    }

    // Merge allowed options
    command = mergeFindAndModifyOptions(command, options, ['sort']);

    return new Promise(function(resolve, reject) {
      self.topology.command(self.ns, command, options, function(err, r) {
        if(err) return reject(err);
        resolve(r);
      });
    });    
  }

  /**
   * Find a document and replace it in one atomic operation, requires a write lock for the duration of the operation.
   *
   * @method
   * @param {object} filter Document selection filter.
   * @param {object} replacement Document replacing the matching document.
   * @param {object} [options=null] Optional settings.
   * @param {object} [options.projection=null] Limits the fields to return for all matching documents.
   * @param {object} [options.sort=null] Determines which document the operation modifies if the query selects multiple documents.
   * @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
   * @param {boolean} [options.upsert=false] Upsert the document if it does not exist.
   * @param {boolean} [options.returnDocument=true] When false, returns the updated document rather than the original. The default is true.
   * @return {Promise}
   */
  findOneAndReplace(filter, replacement, options) {
    var self = this;
    options = options || {};
    options = shallowClone(options);

    var command = {
      query: filter, update: replacement, new: false
    }

    options.new = options.returnDocument;

    // Merge allowed options
    command = mergeFindAndModifyOptions(command, options, ['sort', 'new', 'fields', 'upsert']);

    return new Promise(function(resolve, reject) {
      self.topology.command(self.ns, command, options, function(err, r) {
        if(err) return reject(err);
        resolve(r);
      });
    });    
  }

  /**
   * Find a document and update it in one atomic operation, requires a write lock for the duration of the operation.
   *
   * @method
   * @param {object} filter Document selection filter.
   * @param {object} update Update operations to be performed on the document
   * @param {object} [options=null] Optional settings.
   * @param {object} [options.projection=null] Limits the fields to return for all matching documents.
   * @param {object} [options.sort=null] Determines which document the operation modifies if the query selects multiple documents.
   * @param {number} [options.maxTimeMS=null] The maximum amount of time to allow the query to run.
   * @param {boolean} [options.upsert=false] Upsert the document if it does not exist.
   * @param {boolean} [options.returnDocument=true] When false, returns the updated document rather than the original. The default is true.
   * @return {Promise}
   */
  findOneAndUpdate(filter, update, options) {
    var self = this;
    options = options || {};
    options = shallowClone(options);

    var command = {
      query: filter, update: replacement, new: true
    }

    options.new = options.returnDocument;

    // Merge allowed options
    command = mergeFindAndModifyOptions(command, options, ['sort', 'new', 'fields', 'upsert']);

    return new Promise(function(resolve, reject) {
      self.topology.command(self.ns, command, options, function(err, r) {
        if(err) return reject(err);
        resolve(r);
      });
    });    
  }

  /**
   * Perform a bulkWrite operation without a fluent API
   *
   * Legal operation types are
   *
   *  { insertOne: { document: { a: 1 } } }
   *  { updateOne: { filter: {a:2}, update: {$set: {a:2}}, upsert:true } }
   *  { updateMany: { filter: {a:2}, update: {$set: {a:2}}, upsert:true } }
   *  { deleteOne: { filter: {c:1} } }
   *  { deleteMany: { filter: {c:1} } }
   *  { replaceOne: { filter: {c:3}, replacement: {c:4}, upsert:true}}]
   * 
   *
   * @method
   * @param {object[]} operations Bulk operations to perform.
   * @param {object} [options=null] Optional settings.
   * @param {(number|string)} [options.w=null] The write concern.
   * @param {number} [options.wtimeout=null] The write concern timeout.
   * @param {boolean} [options.j=false] Specify a journal write concern.
   * @param {boolean} [options.serializeFunctions=false] Serialize functions on any object.
   * @return {Promise}
   */
  bulkWrite(operations, options) {
    var self = this;
    options = options || {};
    options = shallowClone(options);
    if(!Array.isArray(operations)) throw new MongoError("operations must be an array of documents");

    return new Promise(function(resolve, reject) {
      var bulk = options.ordered == true || options.ordered == null 
        ? new OrderedBulkOperation(self.topology, self, options) 
        : new UnorderedBulkOperation(self.topology, self, options);
      // for each op go through and add to the bulk
      for(var i = 0; i < operations.length; i++) {
        bulk.raw(operations[i]);
      }

      // Final options for write concern
      var finalOptions = writeConcern(shallowClone(options), self, options);
      var writeCon = finalOptions.writeConcern ? finalOptions.writeConcern : {};

      // Execute the bulk
      bulk.execute(writeCon, function(err, r) {
        if(err) return reject(err);
        r.insertedCount = r.nInserted;
        r.matchedCount = r.nMatched;
        r.modifiedCount = r.nModified || 0;
        r.deletedCount = r.nRemoved;
        r.upsertedCount = r.getUpsertedIds().length;
        r.upsertedIds = {};
        r.insertedIds = {};

        // Inserted documents
        var inserted = r.getInsertedIds();
        // Map inserted ids
        for(var i = 0; i < inserted.length; i++) {
          r.insertedIds[inserted[i].index] = inserted[i]._id;
        }

        // Upserted documents
        var upserted = r.getUpsertedIds();
        // Map upserted ids
        for(var i = 0; i < upserted.length; i++) {
          r.upsertedIds[upserted[i].index] = upserted[i]._id;
        }    

        // Return the results
        resolve(r);
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