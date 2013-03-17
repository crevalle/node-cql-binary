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

Response.Result.read = function (buf, offset, callback) {
  Type.Int.read(buf, offset, function (resultType, messageOffset) {
    var result = new Response.Result(resultType);
    if (result.type == Response.Result.SCHEMA_CHANGE) {
      Type.String.read(buf, messageOffset, function(change, keyspaceOffset) {
        result.change = change;
        Type.String.read(buf, keyspaceOffset, function(keyspace, tableOffset) {
          result.keyspace = keyspace;
          callback(result, tableOffset);
        })
      })
    }
  })
};

module.exports = Response;
