var Type = {}

Type.Int = function (val) {
  this.val = val;
  this.length = Type.Int.size;
}

Type.Int.size = 4;

Type.Int.read = function(buf, offset, callback) {
  var value = buf.readUInt32BE(offset);
  callback(value, offset + Type.Int.size);
}

Type.Int.prototype.write = function (buf, offset) {
  buf.writeUInt32BE(this.val, offset);
  return offset + Type.Int.size;
}

Type.LongString = function (str) {
  this.str = str;
  this.size = str.length;
  this.length = Type.Int.size + str.length;
}

Type.LongString.read = function (buf, offset, callback) {
  Type.Int.read(buf, offset, function (size, stringOffset) {
    var finalOffset = stringOffset + size
      , string = buf.toString('UTF8', stringOffset, finalOffset);
    callback(string, finalOffset);
  });
};

Type.LongString.prototype.write = function (buf, offset) {
  offset = new Type.Int(this.str.length).write(buf, offset)
  return offset + buf.write(this.str, offset);
};

Type.Map = function (map) {
  var size = 0, value, pair;
  this.pairs = [];
  for (var key in map) {
    value = map[key].toString();
    pair = [new Type.String(key), new Type.String(value)];
    this.pairs.push(pair);
    size += pair[0].length + pair[1].length
  }
  this.length = Type.Short.size + size;
}

Type.Map.read = function (buf, offset, callback) {
  Type.Short.read(buf, offset, function (length, pairsOffset) {
    var i = 0
      , map = {}
      , readNextPair = function (keyOffset) {
        if (i == length) {
          callback(map, keyOffset);
        } else {
          i = i + 1;
          Type.String.read(buf, keyOffset, function (key, valueOffset) {
            Type.String.read(buf, valueOffset, function (value, nextPairOffset) {
              map[key] = value;
              readNextPair(nextPairOffset);
            });
          });
        }
      }
    readNextPair(pairsOffset);
  });
}

Type.Map.prototype.write = function (buf, offset) {
  offset = new Type.Short(this.pairs.length).write(buf, offset);
  this.pairs.forEach(function (pair) {
    offset = pair[0].write(buf, offset);
    offset = pair[1].write(buf, offset);
  });
  return offset;
}

Type.Short = function (val) {
  this.val = val;
  this.length = Type.Short.size;
}

Type.Short.size = 2;

Type.Short.read = function(buf, offset, callback) {
  var value = buf.readUInt16BE(offset);
  callback(value, offset + Type.Short.size);
}

Type.Short.prototype.write = function (buf, offset) {
  buf.writeUInt16BE(this.val, offset);
  return offset + Type.Short.size;
}

Type.Consistency = Type.Short;

Type.String = function (str) {
  this.str = str;
  this.size = str.length;
  this.length = Type.Short.size + str.length;
}

Type.String.read = function (buf, offset, callback) {
  Type.Short.read(buf, offset, function (size, stringOffset) {
    var finalOffset = stringOffset + size
      , string = buf.toString('UTF8', stringOffset, finalOffset);
    callback(string, finalOffset);
  });
};

Type.String.prototype.write = function (buf, offset) {
  offset = new Type.Short(this.str.length).write(buf, offset)
  return offset + buf.write(this.str, offset);
};

module.exports = Type;
