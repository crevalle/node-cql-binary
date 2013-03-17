var vows = require('vows'),
    expect = require('expect.js'),
    cqlbinary = require('../../lib/cqlbinary/index');

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
          cqlbinary.CONSISTENCY.ONE,
          this.callback
        );
      },

      'it should send response': function (error, response) {
        expect(error).not.to.be.ok();
        expect(response).to.be.ok();
      },

      'teardown': function () {
        connection = arguments[arguments.length - 1];
        connection.execute("DROP KEYSPACE binarytest", cqlbinary.CONSISTENCY.ONE);
      }
    },

    'teardown': function (connection) {
      connection.disconnect();
    }
  },
}).run();
