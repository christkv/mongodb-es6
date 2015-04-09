"use strict";

var co = require('co');

exports['should correctly execute insert methods using crud api'] = {
  metadata: { requires: { } },
  
  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()

      // insertOne
      try { yield client['tests']['t2_3'].drop(); } catch(err){}
      var result = yield client['tests']['t2_3'].insertOne({a:1});
      test.equal(1, result.result.n);
      test.equal(1, result.insertedCount);
      test.ok(result.insertedId != null);

      // insertMany
      try { yield client['tests']['t2_4'].drop(); } catch(err){}
      var result = yield client['tests']['t2_4'].insertMany([{a:1}, {a:2}]);
      test.equal(2, result.result.n);
      test.equal(2, result.insertedCount);
      test.equal(2, result.insertedIds.length);

      //
      // Bulk write method unordered
      // -------------------------------------------------
      try { yield client['tests']['t2_5'].drop(); } catch(err){}
      var result = yield client['tests']['t2_5'].insertMany([{c:1}]);
      test.equal(1, result.result.n);

      var result = yield client['tests']['t2_5'].bulkWrite([
          { insertOne: { a: 1 } }
        , { insertMany: [{ g: 1 }, { g: 2 }] }
        , { updateOne: { q: {a:2}, u: {$set: {a:2}}, upsert:true } }
        , { updateMany: { q: {a:2}, u: {$set: {a:2}}, upsert:true } }
        , { deleteOne: { q: {c:1} } }
        , { deleteMany: { q: {c:1} } }
      ], {ordered:false});

      test.equal(3, result.nInserted);
      test.equal(1, result.nUpserted);
      test.equal(1, result.nRemoved);

      // Crud fields
      test.equal(3, result.insertedCount);
      test.equal(3, Object.keys(result.insertedIds).length);
      test.equal(1, result.matchedCount);
      test.equal(1, result.deletedCount);
      test.equal(1, result.upsertedCount);
      test.equal(1, Object.keys(result.upsertedIds).length);

      //
      // Bulk write method unordered
      // -------------------------------------------------
      try { yield client['tests']['t2_6'].drop(); } catch(err){}
      var result = yield client['tests']['t2_6'].insertMany([{c:1}, {c:2}, {c:3}]);
      test.equal(3, result.result.n);

      var result = yield client['tests']['t2_6'].bulkWrite([
          { insertOne: { document: { a: 1 } } }
        , { updateOne: { filter: {a:2}, update: {$set: {a:2}}, upsert:true } }
        , { updateMany: { filter: {a:3}, update: {$set: {a:3}}, upsert:true } }
        , { deleteOne: { filter: {c:1} } }
        , { deleteMany: { filter: {c:2} } }
        , { replaceOne: { filter: {c:3}, replacement: {c:4}, upsert:true } }
      ], {ordered: false});

      test.equal(1, result.nInserted);
      test.equal(2, result.nUpserted);
      test.equal(2, result.nRemoved);

      // Crud fields
      test.equal(1, result.insertedCount);
      test.equal(1, Object.keys(result.insertedIds).length);
      test.equal(1, result.matchedCount);
      test.equal(2, result.deletedCount);
      test.equal(2, result.upsertedCount);
      test.equal(2, Object.keys(result.upsertedIds).length);

      //
      // Bulk write method ordered
      // -------------------------------------------------
      try { yield client['tests']['t2_7'].drop(); } catch(err){}
      var result = yield client['tests']['t2_7'].insertMany([{c:1}]);
      test.equal(1, result.result.n);

      var result = yield client['tests']['t2_7'].bulkWrite([
          { insertOne: { a: 1 } }
        , { insertMany: [{ g: 1 }, { g: 2 }] }
        , { updateOne: { q: {a:2}, u: {$set: {a:2}}, upsert:true } }
        , { updateMany: { q: {a:2}, u: {$set: {a:2}}, upsert:true } }
        , { deleteOne: { q: {c:1} } }
        , { deleteMany: { q: {c:1} } }
      ], {ordered: true});

      test.equal(3, result.nInserted);
      test.equal(1, result.nUpserted);
      test.equal(1, result.nRemoved);

      // Crud fields
      test.equal(3, result.insertedCount);
      test.equal(3, Object.keys(result.insertedIds).length);
      test.equal(1, result.matchedCount);
      test.equal(1, result.deletedCount);
      test.equal(1, result.upsertedCount);
      test.equal(1, Object.keys(result.upsertedIds).length);

      //
      // Bulk write method ordered
      // -------------------------------------------------
      try { yield client['tests']['t2_8'].drop(); } catch(err){}
      var result = yield client['tests']['t2_8'].insertMany([{c:1}]);
      test.equal(1, result.result.n);

      var result = yield client['tests']['t2_8'].bulkWrite([
          { insertOne: { document: { a: 1 }} }
        , { updateOne: { filter: {a:2}, update: {$set: {a:2}}, upsert:true } }
        , { updateMany: { filter: {a:2}, update: {$set: {a:2}}, upsert:true } }
        , { deleteOne: { filter: {c:1} } }
        , { deleteMany: { filter: {c:1} } }
        , { replaceOne: { filter: {c:3}, replacement: {c:4}, upsert:true } }
      ], {ordered: true});

      test.equal(1, result.nInserted);
      test.equal(2, result.nUpserted);
      test.equal(1, result.nRemoved);

      // Crud fields
      test.equal(1, result.insertedCount);
      test.equal(1, Object.keys(result.insertedIds).length);
      test.equal(1, result.matchedCount);
      test.equal(1, result.deletedCount);
      test.equal(2, result.upsertedCount);
      test.equal(2, Object.keys(result.upsertedIds).length);

      client.close();
      test.done();      
    });
  }
}

exports['should correctly execute update methods using crud api'] = {
  metadata: {},
  
  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()

      //
      // updateOne
      // -------------------------------------------------
      try { yield client['tests']['t3_2'].drop(); } catch(err){}
      var result = yield client['tests']['t3_2'].insertMany([{c:1}]);
      test.equal(1, result.result.n);

      var result = yield client['tests']['t3_2'].updateOne(
          { a: 1 }
        , { $set: { a: 1 } }
        , { upsert: true });
      test.equal(1, result.result.n);
      test.equal(1, result.matchedCount);
      test.ok(result.upsertedId != null);

      var result = yield client['tests']['t3_2'].updateOne(
          { c: 1 }
        , { $set: { a: 1 } });
      test.equal(1, result.result.n);
      test.equal(1, result.matchedCount);
      test.ok(result.upsertedId == null);

      //
      // replaceOne
      // -------------------------------------------------
      var result = yield client['tests']['t3_2'].replaceOne(
          { g : 1 }
        , { g : 2 }
        , { upsert: true });
      test.equal(1, result.result.n);
      test.equal(1, result.matchedCount);
      test.ok(result.upsertedId != null);

      var result = yield client['tests']['t3_2'].replaceOne(
          { g : 2 }
        , { g : 3 }
        , { upsert: true });
      test.equal(1, result.result.n);
      test.ok(result.result.upserted == null);

      test.equal(1, result.matchedCount);
      test.ok(result.upsertedId == null);

      //
      // updateMany
      // -------------------------------------------------
      try { yield client['tests']['t3_2'].drop(); } catch(err){}
      var result = yield client['tests']['t3_2'].insertMany([{a:1}, {a:1}]);
      test.equal(2, result.result.n);

      var result = yield client['tests']['t3_2'].updateMany(
          { a : 1 }
        , { $set: { a: 2 } }
        , { upsert: true });
      test.equal(2, result.result.n);
      test.equal(2, result.matchedCount);
      test.ok(result.upsertedId == null);

      var result = yield client['tests']['t3_2'].updateMany(
          { c : 1 }
        , { $set: { d: 2 } }
        , { upsert: true });
      test.equal(1, result.matchedCount);
      test.ok(result.upsertedId != null);

      client.close();
      test.done();      
    });
  }
}

exports['should correctly execute remove methods using crud api'] = {
  metadata: {},
  
  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()
      
      try { yield client['tests']['t4_1'].drop(); } catch(err){}
      var result = yield client['tests']['t4_1'].insertMany([{a:1}, {a:1}]);
      test.equal(2, result.result.n);

      var result = yield client['tests']['t4_1'].deleteOne({ a : 1 });
      test.equal(1, result.result.n);
      test.equal(1, result.deletedCount);

      try { yield client['tests']['t4_1'].drop(); } catch(err){}
      var result = yield client['tests']['t4_1'].insertMany([{a:1}, {a:1}]);
      test.equal(2, result.result.n);

      var result = yield client['tests']['t4_1'].deleteMany({ a : 1 });
      test.equal(2, result.result.n);
      test.equal(2, result.deletedCount);

      try { yield client['tests']['t4_1'].drop(); } catch(err){}
      var result = yield client['tests']['t4_1'].insertMany([{a:1}, {a:1}]);
      test.equal(2, result.result.n);

      var result = yield client['tests']['t4_1'].deleteMany();
      // test.equal(2, result.result.n);
      // test.equal(2, result.deletedCount);

      client.close();
      test.done();      
    });
  }
}

exports['should correctly execute findAndModify methods using crud api'] = {
  metadata: {},
  
  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('../..').MongoClient;

    co(function* () {
      // Connect
      var client = yield new MongoClient('mongodb://localhost:27017/test', {}).connect()

      //
      // findOneAndDelete method
      // -------------------------------------------------
      try { yield client['tests']['t5_1'].drop(); } catch(err){}
      var result = yield client['tests']['t5_1'].insertMany([{a:1, b:1}]);
      test.equal(1, result.result.n);

      var result = yield client['tests']['t5_1'].findOneAndDelete(
          { a : 1 }
        , { projection: {b:1}, sort: {a:1} });
      test.equal(1, result.result.lastErrorObject.n);
      test.equal(1, result.result.value.b);

      //
      // findOneAndReplace method
      // -------------------------------------------------
      try { yield client['tests']['t5_1'].drop(); } catch(err){}
      var result = yield client['tests']['t5_1'].insertMany([{a:1, b:1}]);
      test.equal(1, result.result.n);

      var result = yield client['tests']['t5_1'].findOneAndReplace(
          { a : 1 }
        , {c:1, b:1}
        , { projection: {b:1}, sort: {a:1}, returnOriginal: false, upsert: true });
      test.equal(1, result.result.lastErrorObject.n);
      test.equal(1, result.result.value.b);
      test.equal(1, result.result.value.c);

      //
      // findOneAndRemove method
      // -------------------------------------------------
      try { yield client['tests']['t5_1'].drop(); } catch(err){}
      var result = yield client['tests']['t5_1'].insertMany([{a:1, b:1}]);
      test.equal(1, result.result.n);

      var result = yield client['tests']['t5_1'].findOneAndUpdate(
          {a:1}
        , {$set: {d:1}}
        , { projection: {b:1, d:1}, sort: {a:1}, returnOriginal: false, upsert: true });
      test.equal(1, result.result.lastErrorObject.n);
      test.equal(1, result.result.value.b);
      test.equal(1, result.result.value.d);

      client.close();
      test.done();      
    });
  }
}