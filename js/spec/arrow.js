// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

var fs = require('fs');
var chai = require('chai');
var assert = chai.assert;
var path= require('path');
var arrow = require('../dist/arrow.js');

test_files = [
  {
    name: 'simple',
    batches: 1,
    fields: [
      {
        "name": "foo",
        "type": "Int",
        "data": [[1, null, 3, 4, 5]]
      },
      {
        "name": "bar",
        "type": "FloatingPoint",
        "data": [[1.0, null, null, 4.0, 5.0]]
      },
      {
        "name": "baz",
        "type": "Utf8",
        "data": [["aa", null, null, "bbb", "cccc"]]
      }
    ]
  },
  {
    name: 'struct_example',
    batches: 2,
    fields: [
      {
        "name": "struct_nullable",
        "type": "Struct",
        "data": [
          [
            null,
            [null, 'MhRNxD4'],
            [137773603, '3F9HBxK'],
            [410361374, 'aVd88fp'],
            null,
            [null, '3loZrRf'],
            null
          ], [
            null,
            [null,null],
            [null,null],
            null,
            [null, '78SLiRw'],
            null,
            null,
            [null, '0ilsf82'],
            [null, 'LjS9MbU'],
            [null, null],
          ]
        ]
      }
    ]
  },
  {
    name: 'dictionary',
    batches: 2,
    fields: [
      {
        "name": "example-csv",
        "type": "Struct",
        "data": [
          [
            ["Hermione", 25, new Float32Array([-53.235599517822266, 40.231998443603516])],
            ["Severus", 30, new Float32Array([-62.22999954223633, 3])],
          ], [
            ["Harry", 20, new Float32Array([23, -100.23652648925781])]
          ]
        ]
      }
    ]
  },
];

var buf;

function makeSchemaChecks(fields) {
  describe('schema', function () {
    var schema;
    beforeEach(function () {
      schema = arrow.getSchema(buf);
    });

    it('should read the number of fields', function () {
        assert.lengthOf(schema, fields.length);
    });

    it("should understand fields", function () {
      for (i = 0; i < fields.length; i += 1|0) {
          assert.equal(schema[i].name, fields[i].name);
          assert.equal(schema[i].type, fields[i].type,
                       'bad type for field ' + schema[i].name);
      }
    });
  });
}

function makeDataChecks (batches, fields) {
  describe('data', function() {
    var reader;
    beforeEach(function () {
        reader = arrow.getReader(buf)
    });
    it('should read the correct number of record batches', function () {
        assert.equal(reader.getBatchCount(), batches);
    });
    fields.forEach(function (field, i) {
      it('should read ' + field.type + ' vector ' + field.name, function () {
        for (var batch_idx = 0; batch_idx < batches; batch_idx += 1|0) {
          reader.loadNextBatch();
          var batch = field.data[batch_idx];
          var vector = reader.getVector(field.name)
          assert.isDefined(vector, "vector " + field.name);
          assert.lengthOf(vector, batch.length, "vector " + field.name)
          for (i = 0; i < vector.length; i += 1|0) {
            if (field.type == "Date") {
              assert.equal(vector.get(i).getTime(), batch[i].getTime(),
                           "vector " + field.name + " index " + i);
            } else {
              assert.deepEqual(vector.get(i), batch[i],
                               "vector " + field.name + " index " + i);
            }
          }
        }
      });
    });
  });
}

describe('arrow random-access file', function () {
  test_files.forEach(function (test_file) {
    describe(test_file.name, function () {
      var fields = test_file.fields
      beforeEach(function () {
        buf = fs.readFileSync(path.resolve(__dirname, test_file.name + '.arrow'));
      });

      makeSchemaChecks(fields);
      makeDataChecks(test_file.batches, fields);
    })
  });
});

describe('arrow streaming file format', function () {
  test_files.forEach(function (test_file) {
    describe(test_file.name, function () {
      var fields = test_file.fields
      beforeEach(function () {
        buf = fs.readFileSync(path.resolve(__dirname, test_file.name + '-stream.arrow'));
      });

      makeSchemaChecks(fields);
      makeDataChecks(test_file.batches, fields);
    })
  });
});
