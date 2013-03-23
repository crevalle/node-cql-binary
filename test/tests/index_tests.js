var vows = require('vows'),
    expect = require('expect.js'),
    cqlbinary = require('../../lib/cqlbinary/index'),
    CONSISTENCY = cqlbinary.CONSISTENCY,
    Result = cqlbinary.Result;

var assertSuccess = function (error, result) {
  expect(error).to.not.be.ok();
  expect(result).to.be.ok();
};

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

      'it should send response': assertSuccess,

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
        expect(result.table).to.be(undefined);
      },

      'with keyspace selected': {
        topic: function (_, connection) {
          connection.execute("USE binarytest", CONSISTENCY.ONE, this.callback);
        },

       'it should complete successfully': assertSuccess,

        'it should yield result of type SET_KEYSPACE': function (error, result) {
          expect(result.type).to.be(Result.SET_KEYSPACE);
        },

        'it should put keyspace in result': function (error, result) {
          expect(result.keyspace).to.be('binarytest');
        },

        'with table created': {
          topic: function (_, _, connection) {
            connection.execute(
              "CREATE TABLE funny_table (id int PRIMARY KEY, name varchar)",
              CONSISTENCY.ONE,
              this.callback
            );
          },

          'it should complete successfully': assertSuccess,

          'it should return result of type schema change': function (error, result) {
            expect(result.type).to.be(Result.SCHEMA_CHANGE);
          },

          'it should put change in result': function (error, result) {
            expect(result.change).to.be('CREATED');
          },

          'it should put keyspace in result': function (error, result) {
            expect(result.keyspace).to.be('binarytest');
          },

          'it should put table name in result': function (error, result) {
            expect(result.table).to.be('funny_table');
          },

          'with row added': {
            topic: function (_, _, _, connection) {
              connection.execute(
                "INSERT INTO funny_table (id, name) VALUES (1, 'foo')",
                CONSISTENCY.ONE,
                this.callback
              );
            },

            'it should complete successfully': assertSuccess,

            'it should return result of type VOID': function (error, result) {
              expect(result.type).to.be(Result.VOID);
            },

            'reading a row': {
              topic: function (_, _, _, _, connection) {
                connection.execute(
                  "SELECT * FROM funny_table WHERE id = 1 LIMIT 1",
                  CONSISTENCY.ONE,
                  this.callback
                );
              },

              'it should complete successfully': assertSuccess,

              'it should return result of type rows': function (error, result) {
                expect(result.type).to.be(Result.ROWS);
              },

              'it should return the row': function (error, result) {
                expect(result.rows).to.eql([{id: 1, name: "foo"}]);
              }
            }
          },
        },
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
