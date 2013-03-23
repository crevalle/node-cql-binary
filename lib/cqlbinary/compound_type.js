var CompoundType = function (types) {
  var compound_type = function (args) {
    var i = 0, length = 0;
    this.values = types.map(function (type) {
      return new type(args[i++]);
    });
    this.values.forEach(function (value) { length = length + value.length });
    this.length = length;
  };

  compound_type.read = function (reader) {
    var i = 0, values = new Array(types.length), type;

    for (i = 0; i < types.length; i++) {
      type = types[i];
      values[i] = type.read(reader);
    }
    return values;
  };

  compound_type.prototype.write = function (writer) {
    this.values.forEach(function (value) {
      value.write(writer);
    });
    return writer.offset;
  }

  return compound_type;
};

module.exports = CompoundType;
