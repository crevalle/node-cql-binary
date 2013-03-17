var net = require('net')
  , Type = require('./type')
  , CompoundType = require('./compound_type');

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

var CONSISTENCY = {
  ANY: 0x0000,
  ONE: 0x0001,
  TWO: 0x0002,
  THREE: 0x0003,
  QUORUM: 0x0004,
  ALL: 0x0005,
  LOCAL_QUORUM: 0x0006,
  EACH_QUORUM: 0x0007
};

var HEADER_LENGTH = 8;

var Connection = function (onReady) {
  var connection = this;
  this.socket = net.connect(
    {host: "127.0.0.1", port: 9042},
    function () {
      var message = new Request.Startup({CQL_VERSION: "3.0.0"});
      connection.send(OPCODE.STARTUP, message, function (response) {
        if (response.opcode == OPCODE.READY) {
          onReady();
        } else {
          throw "Unexpected response code: " + response.opcode;
        }
      });
    }
  );
}

var Message = function (objects) {
  this.objects = objects;
};

var Bullshit = {read: function (buf, offset, callback) { callback(buf.toString('UTF-8', offset)) }};

var Request = {
  Startup: Type.Map,
  Query: new CompoundType([Type.LongString, Type.Consistency])
}

var Response = {
  Error: new CompoundType([Type.Int, Type.String])
}

Connection.prototype.send = function (opcode, message, onResponse) {
  var connection = this;
  var requestFrame = this.createRequestFrame(opcode, message);
  var socket = this.socket;
  socket.write(requestFrame, function () {
    socket.on('data', function (data) {
      connection.readResponseFrame(data, function (response) {
        if (response.opcode == OPCODE.ERROR) {
          var error = response.body;
          throw "Server Error " + error[0] + ": " + error[1];
        } else {
          onResponse(response);
        }
      });
    });
  });
};

Connection.prototype.execute = function (query, consistency, handleResponse) {
  var message = new Request.Query([query, consistency]);
  this.send(OPCODE.QUERY, message, function (response) {
    if (handleResponse !== undefined) handleResponse(response);
  });
};

Connection.prototype.createRequestFrame = function (opcode, message) {
  var buffer = new Buffer(HEADER_LENGTH + message.length);
  buffer.writeUInt8(1, 0); // Request
  buffer.writeUInt8(0, 1); //FIXME Flags
  buffer.writeUInt8(1, 2); // FIXME Stream
  buffer.writeUInt8(opcode, 3);
  buffer.writeInt32BE(message.length, 4);
  message.write(buffer, HEADER_LENGTH);
  return buffer;
};

Connection.prototype.readResponseFrame = function (buffer, callback) {
  var response = {};
  response.flags = buffer.readUInt8(1);
  response.stream = buffer.readUInt8(2);
  response.opcode = buffer.readUInt8(3);
  var bodyType;
  switch (response.opcode) {
    case OPCODE.ERROR:
      bodyType = Response.Error;
      break;
    case OPCODE.RESULT:
      bodyType = Bullshit;
      break;
  }
  if (bodyType) {
    bodyType.read(buffer, HEADER_LENGTH, function (body) {
      response.body = body;
      callback(response);
    });
  } else {
    callback(response);
  }
};

var connect = function (onReady) {
  var connection = new Connection(function () { onReady(connection) });
};

connect(function (connection) {
  connection.execute("DROP KEYSPACE poop", CONSISTENCY.ONE, function () {
    connection.execute(
      "CREATE KEYSPACE poop WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
      CONSISTENCY.ONE,
      function (response) {
        console.log(response);
      }
    );
  })
});