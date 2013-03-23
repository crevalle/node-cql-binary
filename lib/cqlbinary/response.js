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
  var result, table;
  resultType = Type.Int.read(reader);
  result = new Response.Result(resultType);

  switch (result.type) {
    case Response.Result.SCHEMA_CHANGE:
      result.change = Type.String.read(reader);
      result.keyspace = Type.String.read(reader);

      table = Type.String.read(reader);
      if (table) { result.table = table }
      break;
    case Response.Result.SET_KEYSPACE:
      result.keyspace = Type.String.read(reader);
      break;
  }

  return result;
};

module.exports = Response;

