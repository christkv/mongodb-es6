"use strict"

let shallowClone = function(obj) {
  let copy = {};
  for(let name in obj) copy[name] = obj[name];
  return copy;
}

let merge = function(obj1, obj2) {
  var obj1copy = shallowClone(obj1);
  for(var name in obj2) {
    obj1copy[name] = obj2[name];
  }

  return obj1copy
}

exports.shallowClone = shallowClone;
exports.merge = merge;