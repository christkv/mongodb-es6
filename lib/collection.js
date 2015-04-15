"use strict"

let f = require('util').format
  , shallowClone = require('./utils').shallowClone
  , writeConcern = require('./bulk/common').writeConcern
  , ObjectId = require('mongodb-core').BSON.ObjectId
  , OrderedBulkOperation = require('./bulk/ordered')
  , UnorderedBulkOperation = require('./bulk/unordered')
  , AggregationCursor = require('./aggregation_cursor')
  , CommandCursor = require('./command_cursor')
  , Cursor = require('./cursor');

let decorateUpdateWriteResults = function(r) {
  r.matchedCount = r.result.n;
  r.modifiedCount = r.result.nModified != null ? r.result.nModified : r.result.n;
  r.upsertedId = Array.isArray(r.result.upserted) && r.result.upserted.length > 0 ? r.result.upserted[0] : null;
  r.upsertedCount = Array.isArray(r.result.upserted) && r.result.upserted.length ? r.result.upserted.length : 0;
  return r;
}

let mergeFindAndModifyOptions = function(command, options, names) {
  for(let n of names) {
    if(options[n] != null) command[n] = options[n];
  }

  return command;
}

let generateName = function(keys) {
  let names = [];

  for(let name in keys) {
    names.push(f('%s_%s', name, keys[name]))
  }

  return names.join('_');
}

let mergeWriteConcern = function(object, merge) {
  if(object.writeConcern) return object;
  object.writeConcern = merge.writeConcern;
  return object;
}

/**
 * Create a new Collection instance (INTERNAL TYPE, do not instantiate directly)
 * @class
 * @property {string} dbName Get the db name.
 * @property {string} collectionName Get the collection name.
 * @property {object} topology The current associated topology.
 * @return {Collection} a Collection instance.
 */
class Collection {
  constructor(dbName, collectionName, topology, options) {
    options = options || {};
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.ns = f('%s.%s', dbName, collectionName);
    this.topology = topology;
    this.options = options;
    this.promise = options.promise || Promise;
  }

  insertOne(doc, options) {
    options = options != null ? options : {};
    let self = this;
    if(doc._id == null) doc._id = new ObjectId();

    // Merge the write concern
    options = mergeWriteConcern(options, self.options);

    // Return the promise
    return new this.promise(function(resolve, reject) {
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

    return new this.promise(function(resolve, reject) {
      // Add _id if not specified
      for(let i = 0; i < docs.length; i++) {
        if(docs[i]._id == null) docs[i]._id = new ObjectId();
      }

      // Merge the write concern
      options = mergeWriteConcern(options, self.options);

      // Return the promise
      self.topology.insert(self.ns, docs, options, function(err, r) {
        if(err) return reject(err);
        r.insertedCount = r.result.n;
        let ids = [];
        for(let i = 0; i < docs.length; i++) {
          if(docs[i]._id) ids.push(docs[i]._id);
        }
        r.insertedIds = ids;
        resolve(r);
      });
    });
  }

  /**
   * Execute an aggregation framework pipeline against the collection, needs MongoDB >= 2.2
   * @method
   * @param {object} pipeline Array containing all the aggregation framework commands for the execution.
   * @param {object} [options=null] Optional settings.
   * @param {(ReadPreference|string)} [options.readPreference=null] The preferred read preference (ReadPreference.PRIMARY, ReadPreference.PRIMARY_PREFERRED, ReadPreference.SECONDARY, ReadPreference.SECONDARY_PREFERRED, ReadPreference.NEAREST).
   * @param {object} [options.cursor=null] Return the query as cursor, on 2.6 > it returns as a real cursor on pre 2.6 it returns as an emulated cursor.
   * @param {number} [options.cursor.batchSize=null] The batchSize for the cursor
   * @param {boolean} [options.explain=false] Explain returns the aggregation execution plan (requires mongodb 2.6 >).
   * @param {boolean} [options.allowDiskUse=false] allowDiskUse lets the server know if it can use disk to store temporary results for the aggregation (requires mongodb 2.6 >).
   * @param {number} [options.maxTimeMS=null] maxTimeMS specifies a cumulative time limit in milliseconds for processing operations on the cursor. MongoDB interrupts the operation at the earliest following interrupt point.
   * @return {Promise}
   */
  aggregate(pipeline, options) {
    let self = this;
    options = options != null ? options : {};

    // Set the AggregationCursor constructor
    options.cursorFactory = AggregationCursor;
    options.promise = this.options.promise;

    // Build the command
    let command = { aggregate : this.collectionName, pipeline : pipeline || []};
    // Return an aggregation cursor    
    return this.topology.cursor(this.ns, command, options);
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
    let self = this;
    options = options || {};
    options = shallowClone(options);

    // Merge the write concern
    options = mergeWriteConcern(options, self.options);

    // Return the promise
    return new this.promise(function(resolve, reject) {
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
    let self = this;
    options = options || {};
    options = shallowClone(options);

    // Merge the write concern
    options = mergeWriteConcern(options, self.options);

    // Return the promise
    return new this.promise(function(resolve, reject) {
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
    let self = this;
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
    let self = this;
    options = options || {};
    options = shallowClone(options);

    // Merge the write concern
    options = mergeWriteConcern(options, self.options);

    // Return the promise
    return new this.promise(function(resolve, reject) {
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
    let self = this;
    options = options || {};
    options = shallowClone(options);

    // No filter defined remove all documents in the collection
    filter = filter || {}

    // Merge the write concern
    options = mergeWriteConcern(options, self.options);

    // Return the promise
    return new this.promise(function(resolve, reject) {
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
    let self = this;
    options = options || {};
    options = shallowClone(options);

    let command = {
      findAndModify: this.collectionName, query: filter, remove: true
    }

    options.fields = options.projection;
    // Merge allowed options
    command = mergeFindAndModifyOptions(command, options, ['sort', 'fields']);

    return new this.promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.dbName), command, options, function(err, r) {
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
    let self = this;
    options = options || {};
    options = shallowClone(options);

    let command = {
      findAndModify: this.collectionName, query: filter, update: replacement, new: true
    }

    options.new = options.returnDocument;

    // Merge allowed options
    command = mergeFindAndModifyOptions(command, options, ['sort', 'new', 'fields', 'upsert']);

    return new this.promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.dbName), command, options, function(err, r) {
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
    let self = this;
    options = options || {};
    options = shallowClone(options);

    let command = {
      findAndModify: this.collectionName, query: filter, update: update, new: true
    }

    options.new = options.returnDocument;

    // Merge allowed options
    command = mergeFindAndModifyOptions(command, options, ['sort', 'new', 'fields', 'upsert']);

    return new this.promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.dbName), command, options, function(err, r) {
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
    let self = this;
    options = options || {};
    options = shallowClone(options);
    if(!Array.isArray(operations)) throw new MongoError("operations must be an array of documents");

    return new this.promise(function(resolve, reject) {
      let bulk = options.ordered == true || options.ordered == null 
        ? new OrderedBulkOperation(self.topology, self, options) 
        : new UnorderedBulkOperation(self.topology, self, options);

      // for each op go through and add to the bulk
      for(let i = 0; i < operations.length; i++) {
        bulk.raw(operations[i]);
      }

      // Final options for write concern
      let finalOptions = writeConcern(shallowClone(options), self, options);
      let writeCon = finalOptions.writeConcern ? finalOptions.writeConcern : {};

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
        let inserted = r.getInsertedIds();
        // Map inserted ids
        for(let i = 0; i < inserted.length; i++) {
          r.insertedIds[inserted[i].index] = inserted[i]._id;
        }

        // Upserted documents
        let upserted = r.getUpsertedIds();
        // Map upserted ids
        for(let i = 0; i < upserted.length; i++) {
          r.upsertedIds[upserted[i].index] = upserted[i]._id;
        }    

        // Return the results
        resolve(r);
      });
    });
  }

  createIndex(keys, options) {
    let self = this;
    options = options || {};

    // Merge all the options into the index
    let index = {key: keys}
    for(let n in options) index[n] = options[n];

    // If no name exists generate one
    if(!options.name) {
      index.name = generateName(keys);
    }

    // Return a promise
    return new this.promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.dbName), {
          createIndexes: self.collectionName
        , indexes: [index]
      }, options, function(err, r) {
        if(err) return reject(err);
        resolve(r);
      });      
    });
  }

  createIndexes(indexes) {
    let self = this;
    // Ensure we generate the correct name if the parameter is not set
    for(let i = 0; i < indexes.length; i++) {
      if(indexes[i].name == null) {
        indexes[i].name = generateName(indexes[i].key);
      }
    }

    return new this.promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.dbName), {
          createIndexes: self.collectionName
        , indexes: indexes
      }, function(err, r) {
        if(err) return reject(err);
        resolve(r);
      });      
    });
  }

  dropIndex(key) {
    let self = this;

    return new this.promise(function(resolve, reject) {
      // Generate the index name
      let indexName = typeof key == 'string' ? key : generateName(key);
      // Drop the index
      self.topology.command(f('%s.$cmd', self.dbName), {
          dropIndexes: self.collectionName
        , index: indexName
      }, function(err, r) {
        if(err) return reject(err);
        resolve(r);
      });            
    });
  }

  dropIndexes() {    
    let self = this;

    return new this.promise(function(resolve, reject) {
      // Drop the index
      self.topology.command(f('%s.$cmd', self.dbName), {
          dropIndexes: self.collectionName
        , index: '*'
      }, function(err, r) {
        if(err) return reject(err);
        resolve(r);
      });
    });
  }

  listIndexes() {
    // Set the CommandCursor constructor
    let options = { cursorFactory: CommandCursor, promise: this.options.promise };
    // Build the command
    let command = { listIndexes : this.collectionName};
    // Return an aggregation cursor    
    return this.topology.cursor(this.ns, command, options);    
  }

  drop() {
    let self = this;

    return new this.promise(function(resolve, reject) {
      self.topology.command(f('%s.$cmd', self.dbName), {drop: self.collectionName}, function(err, r) {
        if(err) return reject(err);
        resolve(r);
      });
    });
  }

  find(query) {
    // Build the find command
    let findCommand = {
        find: this.ns
      , query: query
    }

    // Set the cursor factory we are using
    let options = {cursorFactory: Cursor, promise: this.options.promise};
    // Return a command
    return this.topology.cursor(this.ns, findCommand, options);
  }
}

module.exports = Collection;