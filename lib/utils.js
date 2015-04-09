"use strict"

let shallowClone = function(obj) {
  let copy = {};
  for(let name in obj) copy[name] = obj[name];
  return copy;
}

exports.shallowClone = shallowClone;