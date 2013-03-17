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
    var finalOffset = stringOffset + size;
    var string = buf.toString('UTF8', stringOffset, finalOffset);
    callback(string, finalOffset);
  });
};

Type.LongString.prototype.write = function (buf, offset) {
  offset = new Type.Int(this.str.length).write(buf, offset)
  return offset + buf.write(this.str, offset);
};

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

Type.Map.read = function (buf, offset, callback) {
  Type.Short.read(buf, offset, function (length, pairsOffset) {
    var i = 0;
    var map = {};
    var readNextPair = function (keyOffset) {
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
    var finalOffset = stringOffset + size;
    var string = buf.toString('UTF8', stringOffset, finalOffset);
    callback(string, finalOffset);
  });
};

Type.String.prototype.write = function (buf, offset) {
  offset = new Type.Short(this.str.length).write(buf, offset)
  return offset + buf.write(this.str, offset);
};


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

var CompoundType = function (types) {
  var compound_type = function () {
    var args = arguments;
    var i = 0;
    this.values = types.map(function (type) {
      return new type(args[i++]);
    });
    var length = 0;
    this.values.forEach(function (value) { length = length + value.length });
    this.length = length;
  };

  compound_type.read = function (buf, offset, callback) {
    var i = 0;
    var values = new Array(types.length);
    var readNext = function (valueOffset) {
      if (i == types.length) {
        callback(values);
      } else {
        var type = types[i];
        type.read(buf, valueOffset, function (value, nextOffset) {
          values[i] = value;
          i = i + 1;
          readNext(nextOffset);
        });
      }
    };
    readNext(offset);
  };

  compound_type.prototype.write = function (buf, offset) {
    this.values.forEach(function (value) {
      offset = value.write(buf, offset);
    });
    return offset;
  }

  return compound_type;
}

var Bullshit = {read: function (buf, offset, callback) { callback(buf.toString(offset)) }};

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
  var message = new Request.Query(query, consistency);
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
  connection.execute(
    "CREATE KEYSPACE poop WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    CONSISTENCY.ONE,
    function (response) {
      console.log(response);
    });
});
