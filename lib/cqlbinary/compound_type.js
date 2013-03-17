var CompoundType = function (types) {
  var compound_type = function (args) {
    var i = 0, length = 0;
    this.values = types.map(function (type) {
      return new type(args[i++]);
    });
    this.values.forEach(function (value) { length = length + value.length });
    this.length = length;
  };

  compound_type.read = function (buf, offset, callback) {
    var i = 0
      , values = new Array(types.length)
      , readNext = function (valueOffset) {
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
};

module.exports = CompoundType;
