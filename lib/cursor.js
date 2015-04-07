"use strict"

var CoreCursor = require('mongodb-core').Cursor;

CoreCursor.prototype._next = CoreCursor.prototype.next;

class Cursor extends CoreCursor {
  constructor(bson, ns, cmd, options, topology, topologyOptions) {
    super(bson, ns, cmd, options, topology, topologyOptions);

    // Buffered doc
    this.doc = null;
  }

  next() {
    var self = this;

    return new Promise(function(resolve, reject) {
      // If we have a doc
      if(self.doc) {
        var doc = self.doc;
        self.doc = null;
        return resolve(doc);
      }

      // Execute next
      self._next(function(err, doc) {
        if(err) return reject(err);
        resolve(doc)
      });
    });
  }

  hasNext() {
    var self = this;

    return new Promise(function(resolve, reject) {
      self._next(function(err, doc) {
        if(err) return reject(err);
        if(doc != null) self.doc = doc;
        resolve(doc == null ? false: true);
      });
    });
  }

  toArray() {
    var self = this;
    var items = [];

    // Reset cursor
    this.rewind();

    // Return a promise to allow for yield operations
    return new Promise(function(resolve, reject) {
      // Get all the documents
      var fetchDocs = function() {
        self._next(function(err, doc) {
          if(err) return reject(err);
          if(doc == null) return resolve(items);

          // Add doc to items
          items.push(doc);
          // Get all buffered objects
          if(self.bufferedCount() > 0) {
            var a = self.readBufferedDocuments(self.bufferedCount())
            items = items.concat(a);
          }

          // Attempt a fetch
          fetchDocs();
        });
      }

      fetchDocs();
    });
  }
}

module.exports = Cursor;