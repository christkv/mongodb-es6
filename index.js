"use strict"

var exposed = {
    MongoClient: require('./lib/mongoclient')
  , ReadPreference: require('mongodb-core').ReadPreference
}

module.exports = exposed;