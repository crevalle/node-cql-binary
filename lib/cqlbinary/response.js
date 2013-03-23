var CompoundType = require('./compound_type')
  , Type = require('./type')
  , Option = require('./option')
  , ColumnType = require('./column_type')
  , ColumnTypeOption;

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
  var result, table, globalTableSpec;
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
    case Response.Result.ROWS:
      result.flags = Type.Int.read(reader);
      result.columnsCount = Type.Int.read(reader);

      if (result.flags === 1) {
        globalTableSpec = {
          keyspace: Type.String.read(reader),
          table: Type.String.read(reader)
        };
      }

      var i;
      result.columns = new Array(result.columnsCount);
      for (i = 0; i < result.columnsCount; i++) {

        var column = { }
        if (globalTableSpec) {
          column.keyspace = globalTableSpec.keyspace;
          column.table = globalTableSpec.table;
        } else {
          column.keyspace = Type.String.read(reader);
          column.table = Type.String.read(reader);
        }

        column.name = Type.String.read(reader);
        column.type = columnTypeOption.read(reader).columnType;
        result.columns[i] = column;
      }

      result.rowsCount = Type.Int.read(reader);
      var rowsContent = new Array(result.rowsCount);

      for(i = 0; i < result.rowsCount; i++) {
        var row = {}
        for (var c = 0; c < result.columnsCount; c++) {
          var column = result.columns[c];
          var valueBytes = Type.Bytes.read(reader);
          row[column.name] = column.type.read(valueBytes);
        }
        rowsContent[i] = row;
      }
      result.rows = rowsContent;
      break;
  }

  return result;
};

columnTypeOption = new Option(function (option, reader) {
  if (option.id === 0) {
    option.columnType = Type.String.read(reader);
  } else {
    option.columnType = ColumnType[option.id];
  }
});

module.exports = Response;

