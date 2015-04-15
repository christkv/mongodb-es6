"use strict"

var exposed = {
    MongoClient: require('./lib/mongoclient')
  , ReadPreference: require('mongodb-core').ReadPreference
}

// Add all the types
exposed.BSON = require('mongodb-core').BSON;
exposed.Long = exposed.BSON.Long;
exposed.Binary = exposed.BSON.Binary;
exposed.Code = exposed.BSON.Code;
exposed.DBRef = exposed.BSON.DBRef;
exposed.Double = exposed.BSON.Double;
exposed.MaxKey = exposed.BSON.MaxKey;
exposed.MinKey = exposed.BSON.MinKey;
exposed.ObjectId = exposed.BSON.ObjectId;
exposed.Symbol = exposed.BSON.Symbol;
exposed.Timestamp = exposed.BSON.Timestamp;

module.exports = exposed;