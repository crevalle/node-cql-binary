var vows = require('vows'),
    expect = require('expect.js'),
    cqlbinary = require('../../lib/cqlbinary/index'),
    CONSISTENCY = cqlbinary.CONSISTENCY,
    Result = cqlbinary.Result;

vows.describe('cqlbinary').addBatch({
  'Connection': {
    topic: function () {
      var callback = this.callback;
      cqlbinary.connect(function (error, connection) {
        callback(undefined, connection);
      });
    },

    'it should connect': function (error, connection) {
      expect(connection).to.be.ok();
    },

    'with keyspace': {
      topic: function (connection) {
        connection.execute(
          "CREATE KEYSPACE binarytest WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
          CONSISTENCY.ONE,
          this.callback
        );
      },

      'it should send response': function (error, response) {
        expect(error).not.to.be.ok();
        expect(response).to.be.ok();
      },

      'it should yield result of type SCHEMA_CHANGE': function (error, result) {
        expect(result.type).to.be(Result.SCHEMA_CHANGE);
      },

      'it should put change in result': function (error, result) {
        expect(result.change).to.be('CREATED');
      },

      'it should put keyspace in result': function (error, result) {
        expect(result.keyspace).to.be('binarytest');
      },

      'it should not set table': function (error, result) {
        expect(result.table).not.to.be.ok();
      },

      'teardown': function () {
        connection = arguments[arguments.length - 1];
        connection.execute("DROP KEYSPACE binarytest", CONSISTENCY.ONE);
      }
    },

    'teardown': function (connection) {
      connection.disconnect();
    }
  },
}).run();
