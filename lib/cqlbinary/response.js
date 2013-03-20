var CompoundType = require('./compound_type')
  , Type = require('./type');

var Response = {
  Error: new CompoundType([Type.Int, Type.String]),
  Result: function (type) {
    this.type = type
  }
}

Response.Result.VOID = 1;
Response.Result.ROWS = 2;
Response.Result.SET_KEYSPACE = 3;
Response.Result.PREPARED = 4;
Response.Result.SCHEMA_CHANGE = 5;

Response.Result.read = function (reader) {
  resultType = Type.Int.read(reader);
  var result = new Response.Result(resultType);
  if (result.type == Response.Result.SCHEMA_CHANGE) {
    result.change = Type.String.read(reader);
    result.keyspace = Type.String.read(reader);
  }
  return result;
};

module.exports = Response;
