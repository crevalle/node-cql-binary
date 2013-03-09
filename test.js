var net = require('net');

var OPCODE = {
  ERROR: 0x00,
  STARTUP: 0x01,
  READY: 0x02,
  AUTHENTICATE: 0x03,
  CREDENTIALS: 0x04,
  OPTIONS: 0x05,
  SUPPORTED: 0x06,
  QUERY: 0x07,
  RESULT: 0x08,
  PREPARE: 0x09,
  EXECUTE: 0x0a,
  REGISTER: 0x0b,
  EVENT: 0x0c
};


var HEADER_LENGTH = 8;

var createRequestFrame = function (opcode, body) {
  var buffer = new Buffer(HEADER_LENGTH + body.length);
  buffer.writeUInt8(1, 0); // Request
  buffer.writeUInt8(0, 1); //FIXME Flags
  buffer.writeUInt8(1, 2); // FIXME Stream
  buffer.writeUInt8(opcode, 3);
  buffer.writeInt32BE(body.length, 4);
  body.write(buffer, HEADER_LENGTH);
  console.log(buffer);
  return buffer;
};

var Type = {}

Type.Short = function (val) {
  this.val = val;
  this.length = Type.Short.size;
}

Type.Short.size = 2;

Type.Short.prototype.write = function (buf, offset) {
  console.log("Writing short", this.val, "at offset", offset);
  buf.writeUInt16BE(this.val, offset);
  return offset + Type.Short.size;
}

Type.Map = function (map) {
  this.pairs = [];
  var size = 0
  for (var key in map) {
    var value = map[key].toString();
    var pair = [new Type.String(key), new Type.String(value)];
    this.pairs.push(pair);
    size += pair[0].length + pair[1].length
  }
  this.length = Type.Short.size + size;
}

Type.Map.prototype.write = function (buf, offset) {
  console.log("Writing map", this.pairs, "at offset", offset);
  offset = new Type.Short(this.pairs.length).write(buf, offset);
  this.pairs.forEach(function (pair) {
    offset = pair[0].write(buf, offset);
    offset = pair[1].write(buf, offset);
  });
  return offset;
}

Type.String = function (str) {
  this.str = str;
  this.size = str.length;
  this.length = Type.Short.size + str.length;
}

Type.String.prototype.write = function (buf, offset) {
  console.log("Writing string", this.str, "at offset", offset);
  offset = new Type.Short(this.str.length).write(buf, offset)
  return offset + buf.write(this.str, offset);
};

var cql_connection = function (on_connect) {
  var connection = net.connect(
    {host: "127.0.0.1", port: 9042},
    function () {
      var body = new Type.Map({CQL_VERSION: "3.0.0"});
      var message = createRequestFrame(OPCODE.STARTUP, body);
      connection.write(message, 'UTF8', function () {
        connection.on('data', function (data) {
          console.log('Got Data');
          console.log(data)
          console.log(data.toString());
        });
      });
    }
  );
};

cql_connection();
