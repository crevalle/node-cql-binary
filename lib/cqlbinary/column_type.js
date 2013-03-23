var Type = require("./type")
  , Ascii
  , Bigint
  , Blob
  , Boolean
  , Counter
  , Decimal
  , Double
  , Float
  , Int
  , Text
  , Timestamp
  , Uuid
  , Varchar
  , Varint
  , Timeuuid
  , Inet;

Int = {
  name: "int",
  read: function (bytes) {
    return bytes.readInt32BE(0);
  }
};

Varchar = {
  name: "varchar",
  read: function (bytes) {
    return bytes.toString('UTF-8');
  }
};

module.exports = [
  undefined,
  Ascii,
  Bigint,
  Blob,
  Boolean,
  Counter,
  Decimal,
  Double,
  Float,
  Int,
  Text,
  Timestamp,
  Uuid,
  Varchar,
  Varint,
  Timeuuid,
  Inet
];
