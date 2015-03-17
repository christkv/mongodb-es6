"use strict"

var f = require('util').format;

class Collection {
  constructor(ns, topology) {
    this.ns = ns;
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

  find() {
    // console.log("--------------- find")
  }
}

module.exports = Collection;