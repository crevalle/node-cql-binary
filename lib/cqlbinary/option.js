var Type = require("./type");

var Option = function (readValue) {
  var option = function (id) {
    this.id = id;
  }

  option.read = function (reader) {
    var optionInstance = {id: Type.Short.read(reader)};
    readValue(optionInstance, reader);
    return optionInstance;
  }

  return option;
};

module.exports = Option;
