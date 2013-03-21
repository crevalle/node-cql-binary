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

Type.Int8.prototype.write = function (writer) {
  writer.buffer.writeInt8(this.val, writer.offset);
  writer.offset += Type.Int8.size;
  return writer.offset;
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

Type.Int.prototype.write = function (writer) {
  writer.buffer.writeUInt32BE(this.val, writer.offset);
  writer.offset += Type.Int.size;
  return writer.offset;
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

Type.LongString.prototype.write = function (writer) {
  new Type.Int(this.str.length).write(writer)
  var sizeOfString = writer.buffer.write(this.str, writer.offset);

  writer.offset += sizeOfString;
  return writer.offset;
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

Type.Map.prototype.write = function (writer) {
  new Type.Short(this.pairs.length).write(writer);
  this.pairs.forEach(function (pair) {
    pair[0].write(writer);
    pair[1].write(writer);
  });
  return writer.offset;
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

Type.Short.prototype.write = function (writer) {
  writer.buffer.writeUInt16BE(this.val, writer.offset);
  writer.offset += Type.Short.size;
  return writer.offset;
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

Type.String.prototype.write = function (writer) {
  new Type.Short(this.str.length).write(writer)
  var sizeOfString = writer.buffer.write(this.str, writer.offset);
  writer.offset += sizeOfString;
  return writer.offset;
};

module.exports = Type;
