var Type = {}

Type.Int8 = function (val) {
  this.val = val;
  this.length = Type.Int8.size;
}

Type.Int8.size = 1;

Type.Int8.read = function (reader) {
  var value = reader.buffer.readUInt8(reader.offset);

  reader.offset += Type.Int8.size;
  return value;
}

Type.Int = function (val) {
  this.val = val;
  this.length = Type.Int.size;
}

Type.Int.size = 4;

Type.Int.read = function (reader) {
  var value = reader.buffer.readUInt32BE(reader.offset);

  reader.offset += Type.Int.size;
  return value;
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

Type.LongString.read = function (reader) {
  var sizeOfString = Type.Int.read(reader);
  var finalOffset = reader.offset + sizeOfString
    , string = reader.buffer.toString('UTF8', reader.offset, finalOffset);

  reader.offset += sizeOfString
  return string;
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

Type.Map.read = function (reader) {
  var mapLength = Type.Short.read(reader);
  var i = 0
  , map = {}
  , readNextPair = function (reader) {
    if (i == mapLength) {
      reader.offset += mapLength;
      return map;
    } else {
      i = i + 1;
      var key = Type.String.read(reader);
      var value = Type.String.read(reader);
      map[key] = value;
      readNextPair(reader);
    }
  }
  readNextPair(reader);
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

Type.Short.read = function(reader) {
  var value = reader.buffer.readUInt16BE(reader.offset);

  reader.offset += Type.Short.size;
  return value;
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

Type.String.read = function (reader) {
  sizeOfString = Type.Short.read(reader);
  var finalOffset = reader.offset + sizeOfString
    , string = reader.buffer.toString('UTF8', reader.offset, finalOffset);

  reader.offset += sizeOfString;
  return string;
};

Type.String.prototype.write = function (buf, offset) {
  offset = new Type.Short(this.str.length).write(buf, offset)
  return offset + buf.write(this.str, offset);
};

module.exports = Type;
