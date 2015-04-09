"use strict"

let CoreCursor = require('mongodb-core').Cursor
  , CoreReadPreference = require('mongodb-core').ReadPreference
  , MongoError = require('mongodb-core').MongoError
  , inherits = require('util').inherits
  , Readable = require('stream').Readable
  , f = require('util').format;

CoreCursor.prototype._next = CoreCursor.prototype.next;

// Flags allowed for cursor
let flags = ['tailable', 'oplogReplay', 'noCursorTimeout', 'awaitData', 'exhaust', 'partial'];

// Can we change the cursor
let isDead = function(self) {
  if(self.state == 'closed' || self.state == 'open' || self.isDead()) 
    throw new MongoError(f("cursor is %s or dead", self.state, self.isDead()));
}

class StreamingCursor extends Readable {
  constructor(cursor) {
    super({objectMode:true});
    
    this.cursor = cursor;
  }

  _read(n) {
    let self = this;
    
    if(self.cursor.state == 'closed' || self.cursor.isDead()) {
      return self.push(null);
    }

    // Get the next item
    self.cursor._next(function(err, result) {
      if(err) {
        if(!self.cursor.isDead()) self.cursor.close();
        if(self.listeners('error') && self.listeners('error').length > 0) {
          self.emit('error', err);
        }

        // Emit end event
        return self.emit('end');
      }

      // If we provided a transformation method
      if(typeof self.cursor.transform == 'function' && result != null) {
        return self.push(self.cursor.transform(result));
      }

      // Return the result
      self.push(result);
    });
  }  
}

class Cursor extends CoreCursor {
  constructor(bson, ns, cmd, options, topology, topologyOptions) {
    super(bson, ns, cmd, options, topology, topologyOptions);

    // Stream cursor
    this.stream = new StreamingCursor(this);

    // Save the cmd
    this.cmd = cmd;
    this.options = options;
    this.ns = ns;
    this.topology = topology;
    // Cursor state
    this.state = 'init';
    // Buffered doc
    this.doc = null;
  }

  on(event, callback) {
    this.stream.on(event, callback);
  }

  once(event, callback) {
    this.stream.on(event, callback);
  }

  /**
   * Execute the explain for the cursor
   * @method
   * @return {Promise}
   */
  explain() {
    let self = this;
    self.cmd.explain = true;

    return new Promise(function(resolve, reject) {
      self._next(function(err, doc) {
        if(err) return reject(err);
        resolve(doc);
      });
    });    
  }

  /**
   * Execute the count command for the cursor
   * @method
   * @return {Promise}
   */
  count() {
    let self = this;

    return new Promise(function(resolve, reject) {
      let command = {
          count: self.namespace.collection
        , query: self.cmd.query
        , limit: self.cmd.limit
        , skip: self.cmd.skip
      };

      // Execute count command
      self.topology.command(f('%s.$cmd', self.namespace.database)
        , command, function(err, r) {
          if(err) reject(err);
          resolve(r.result.n);
      });
    });
  }

  /**
   * Checks if there is a document waiting
   * @method
   * @return {Promise}
   */
  hasNext() {
    let self = this;

    return new Promise(function(resolve, reject) {
      self._next(function(err, doc) {
        if(err) return reject(err);
        if(typeof self.transform == 'function' && doc != null) 
          doc = self.transform(doc);
        if(doc != null) self.doc = doc;
        resolve(doc == null ? false: true);
      });
    });
  }

  /**
   * Return all the documents as an array
   * @method
   * @return {Promise}
   */
  toArray() {
    let self = this;
    let items = [];

    // Reset cursor
    this.rewind();

    // Return a promise to allow for yield operations
    return new Promise(function(resolve, reject) {
      // Get all the documents
      let fetchDocs = function() {
        self._next(function(err, doc) {
          if(err) return reject(err);
          if(doc == null) return resolve(items);

          // Transform the doc if transform method added
          if(typeof self.transform == 'function' && doc != null) 
            doc = self.transform(doc);

          // Add doc to items
          items.push(doc);
          // Get all buffered objects
          if(self.bufferedCount() > 0) {
            let a = self.readBufferedDocuments(self.bufferedCount())

            // Transform the array
            if(typeof self.transform == 'function' && doc != null)
              a = a.map(self.transform);

            items = items.concat(a);
          }

          // Attempt a fetch
          fetchDocs();
        });
      }

      fetchDocs();
    });
  }

  /**
   * Return the next document
   * @method
   * @return {Promise}
   */
  next() {
    let self = this;

    return new Promise(function(resolve, reject) {
      if(self.state == 'init') self.state == 'open';
      // If we have a doc
      if(self.doc) {
        let doc = self.doc;
        self.doc = null;
        return resolve(doc);
      }

      // Execute next
      self._next(function(err, doc) {
        if(err) return reject(err);
        if(doc == null) self.state == 'closed';

        // Transform the doc if transform method added
        if(typeof self.transform == 'function' && doc != null) 
          doc = self.transform(doc);

        // Return the doc
        resolve(doc)
      });
    });
  }

  /**
   * Set the cursor query
   * @method
   * @param {object} filter The filter object used for the cursor.
   * @throws {MongoError}
   * @return {Cursor}
   */
  filter(filter) {
    isDead(this);
    this.cmd.query = filter;
    return this;
  }  

  /**
   * Set the cursor hint
   * @method
   * @param {string} hint The index name we wish to use as a hint
   * @throws {MongoError}
   * @return {Cursor}
   */
  hint(hint) {
    isDead(this);
    this.cmd.hint = hint;
    return this;
  }  

  /**
   * Add a cursor flag to the cursor
   * @method
   * @param {string} flag The flag to set, must be one of following ['tailable', 'oplogReplay', 'noCursorTimeout', 'awaitData', 'exhaust', 'partial'].
   * @param {boolean} value The flag boolean value.
   * @throws {MongoError}
   * @return {Cursor}
   */
  addCursorFlag(flag, value) {
    isDead(this);
    if(flags.indexOf(flag) == -1) throw new MongoError(f("flag % not a supported flag %s", flag, flags));
    if(typeof value != 'boolean') throw new MongoError(f("flag % must be a boolean value", flag));
    this.cmd[flag] = value;
    return this;
  }

  /**
   * Add a query modifier to the cursor query
   * @method
   * @param {string} name The query modifier (must start with $, such as $orderby etc)
   * @param {boolean} value The flag boolean value.
   * @throws {MongoError}
   * @return {Cursor}
   */
  addQueryModifier(name, value) {
    isDead(this);
    if(name[0] != '$') throw new MongoError(f("%s is not a valid query modifier"));
    // Strip of the $
    let field = name.substr(1);
    // Set on the command
    this.cmd[field] = value;
    // Deal with the special case for sort
    if(field == 'orderby') this.cmd.sort = this.cmd[field];
    return this;
  }

  /**
   * Add a comment to the cursor query allowing for tracking the comment in the log.
   * @method
   * @param {string} value The comment attached to this query.
   * @throws {MongoError}
   * @return {Cursor}
   */
  comment(value) {
    isDead(this);
    this.cmd.comment = value;
    return this;
  }

  /**
   * Set a maxTimeMS on the cursor query, allowing for hard timeout limits on queries (Only supported on MongoDB 2.6 or higher)
   * @method
   * @param {number} value Number of milliseconds to wait before aborting the query.
   * @throws {MongoError}
   * @return {Cursor}
   */
  maxTimeMS(value) {
    isDead(this);
    if(typeof value != 'number') throw new MongoError("maxTimeMS must be a number");
    this.cmd.maxTimeMS = value;
    return this;
  }

  /**
   * Sets a field projection for the query.
   * @method
   * @param {object} value The field projection object.
   * @throws {MongoError}
   * @return {Cursor}
   */
  project(value) {
    isDead(this);
    this.cmd.fields = value;
    return this;
  }

  /**
   * Sets the sort order of the cursor query.
   * @method
   * @param {(string|array|object)} keyOrList The key or keys set for the sort.
   * @param {number} [direction] The direction of the sorting (1 or -1).
   * @throws {MongoError}
   * @return {Cursor}
   */
  sort(keyOrList, direction) {
    isDead(this);
    if(this.options.tailable) throw new MongoError("Tailable cursor doesn't support sorting");
    let order = keyOrList;

    if(direction != null) {
      order = [[keyOrList, direction]];
    }

    this.cmd.sort = order;
    return this;
  }

  /**
   * Set the batch size for the cursor.
   * @method
   * @param {number} value The batchSize for the cursor.
   * @throws {MongoError}
   * @return {Cursor}
   */
  batchSize(value) {
    isDead(this);
    if(this.options.tailable) throw new MongoError("Tailable cursor doesn't support limit");  
    if(typeof value != 'number') throw new MongoError("batchSize requires an integer");
    this.cmd.batchSize = value;
    this.setCursorBatchSize(value);
    return this;
  }

  /**
   * Set the limit for the cursor.
   * @method
   * @param {number} value The limit for the cursor query.
   * @throws {MongoError}
   * @return {Cursor}
   */
  limit(value) {
    isDead(this);
    if(this.options.tailable) throw new MongoError("Tailable cursor doesn't support limit");
    if(typeof value != 'number') throw new MongoError("limit requires an integer");
    this.cmd.limit = value;
    // this.cursorLimit = value;
    this.setCursorLimit(value);
    return this;
  }

  /**
   * Set a mapping function
   * @method
   * @param {function} transform The transformation method applied to each item
   * @throws {MongoError}
   * @return {Cursor}
   */
  map(transform) {
    isDead(this);
    this.transform = transform;
    return this;
  }

  /**
   * Set the skip for the cursor.
   * @method
   * @param {number} value The skip for the cursor query.
   * @throws {MongoError}
   * @return {Cursor}
   */
  skip(value) {
    isDead(this);
    if(this.options.tailable) throw new MongoError("Tailable cursor doesn't support skip");
    if(typeof value != 'number') throw new MongoError("skip requires an integer");
    this.cmd.skip = value;
    this.setCursorSkip(value);
    return this;
  }

  /**
   * Set the ReadPreference for the cursor.
   * @method
   * @param {(string|ReadPreference)} readPreference The new read preference for the cursor.
   * @throws {MongoError}
   * @return {Cursor}
   */
  setReadPreference(r) {
    if(this.state != 'init') throw new MongoError('cannot change cursor readPreference after cursor has been accessed');
    this.options.readPreference = r;
    return this;
  }  

  get namespace() {
    if(!this) return null;
    let ns = this.ns || '';
    let firstDot = ns.indexOf('.');
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

module.exports = Cursor;