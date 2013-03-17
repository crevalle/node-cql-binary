var vows = require('vows'),
    expect = require('expect.js');

vows.describe('cqlbinary').addBatch({
  'connect': {
    topic: 1,

    'math should be correct': function (topic) {
      expect(topic).to.be(1);
    }
  }
}).run();
