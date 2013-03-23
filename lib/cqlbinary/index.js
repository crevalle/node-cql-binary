var net = require('net')
  , Type = require('./type')
  , CompoundType = require('./compound_type')
  , Response = require('./response');

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
  this.connected = false;
  this.connect(onReady);
};

Connection.prototype.connect = function (onReady) {
  var connection = this;
  this.socket = new net.Socket();

  this.socket.on('connect', function () {
    connection.handleConnect(onReady);
  });

  this.socket.on('data', function (data) {
    connection.handleData(data);
  });

  this.socket.on('end', function () {
    connection.handleDisconnect();
  });

  this.socket.connect(9042, "127.0.0.1");
};

Connection.prototype.handleConnect = function (onReady) {
  var message = new Request.Startup({CQL_VERSION: "3.0.0"});
  this.connected = true;
  this.send(OPCODE.STARTUP, message, function (error, response) {
    if (response.opcode == OPCODE.READY) {
      onReady();
    } else {
      onReady("Unexpected response code: " + response.opcode);
    }
  });
};

Connection.prototype.handleData = function (data) {
  var connection = this;
  var responseHandler = connection.responseHandler;
  connection.responseHandler = undefined;
  try {
    var response = this.readResponseFrame(data);
    if (responseHandler) {
      if (response.opcode == OPCODE.ERROR) {
        var error = response.body;
        responseHandler(error);
      } else {
        responseHandler(undefined, response);
      }
    }
  } catch (e) {
    if (responseHandler) responseHandler(e);
  }
  if (connection.pendingDisconnect) {
    connection.kill();
  }
};

Connection.prototype.handleDisconnect = function () {
  this.connected = false;
};

Connection.prototype.send = function (opcode, message, onResponse) {
  var connection = this
    , requestFrame = this.createRequestFrame(opcode, message)
    , socket = this.socket;

  if (!this.connected) {
    throw "Not connected!";
  }

  socket.write(requestFrame, function () {
    connection.responseHandler = onResponse;
  });
};

Connection.prototype.execute = function (query, consistency, handleResponse) {
  var message = new Request.Query([query, consistency]);
  this.send(OPCODE.QUERY, message, function (error, response) {
    var result;
    if (response) result = response.body;
    if (handleResponse !== undefined) handleResponse(error, result);
  });
};

Connection.prototype.disconnect = function () {
  if (this.responseHandler) {
    this.pendingDisconnect = true;
  } else {
    this.kill();
  }
};

Connection.prototype.kill = function () {
  this.socket.end();
};

Connection.prototype.createRequestFrame = function (opcode, message) {
  var buffer = new Buffer(HEADER_LENGTH + message.length);
  var writer = {
    offset: 0,
    buffer: buffer
  }

  new Type.Int8(1).write(writer) // Request
  new Type.Int8(0).write(writer) // Flags
  new Type.Int8(1).write(writer) // Stream
  new Type.Int8(opcode).write(writer)
  new Type.Int(message.length).write(writer)

  message.write(writer);
  return writer.buffer;
};

Connection.prototype.readResponseFrame = function (buffer) {
  var response = {}, bodyType;
  var reader = {
    offset: 0,
    buffer: buffer
  }

  response.version = Type.Int8.read(reader);
  response.flags  = Type.Int8.read(reader);
  response.stream = Type.Int8.read(reader);
  response.opcode = Type.Int8.read(reader);
  response.length = Type.Int.read(reader);

  switch (response.opcode) {
    case OPCODE.ERROR:
      bodyType = Response.Error;
      break;
    case OPCODE.RESULT:
      bodyType = Response.Result;
      break;
  }
  if (bodyType) {
    response.body = bodyType.read(reader);
  }
  return response;
};

var Message = function (objects) {
  this.objects = objects;
};


var Request = {
  Startup: Type.Map,
  Query: new CompoundType([Type.LongString, Type.Consistency])
}

var connect = function (onReady) {
  var connection = new Connection(function () { onReady(undefined, connection) });
};

module.exports.connect = connect;
module.exports.CONSISTENCY = CONSISTENCY;
module.exports.Result = Response.Result;
