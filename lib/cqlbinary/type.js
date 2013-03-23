var Type = {}

Type.Bytes = function (buf) {
  this.val = buf;
  this.length = buf.length;
}

Type.Bytes.read = function (reader) {
  var size = Type.Int.read(reader);
  if (size >= 0) {
    var bytes = reader.buffer.slice(reader.offset, reader.offset + size);
    reader.offset += size;
    return bytes;
  }
}

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
  return Type.Int8.size;
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
  return Type.Int.size;
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
  var mapLength = Type.Short.read(reader)
    , i, map = {};

  for (i = 0; i < mapLength; i++) {
    var key = Type.String.read(reader);
    var value = Type.String.read(reader);
    map[key] = value;
  }
  return map;
}

Type.Map.prototype.write = function (writer) {
  var sizeOfMap = new Type.Short(this.pairs.length).write(writer);
  this.pairs.forEach(function (pair) {
    sizeOfMap += pair[0].write(writer);
    sizeOfMap += pair[1].write(writer);
  });
  return sizeOfMap;
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
  return Type.Short.size;
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
  var written = 0;
  written += new Type.Short(this.str.length).write(writer);
  var sizeOfString = writer.buffer.write(this.str, writer.offset);
  writer.offset += sizeOfString;
  written += sizeOfString;
  return written;
};

module.exports = Type;
