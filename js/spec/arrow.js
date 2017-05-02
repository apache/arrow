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
var arrow = require('../dist/arrow.js');

fields = [
  {
    "name": "foo",
    "type": "Int",
    "data": [1, null, 3, 4, 5]
  },
  {
    "name": "bar",
    "type": "FloatingPoint",
    "data": [1.0, null, null, 4.0, 5.0]
  },
  {
    "name": "baz",
    "type": "Utf8",
    "data": ["aa", null, null, "bbb", "cccc"]
  }
];

describe('arrow random-access file', function () {
  var buf;
  beforeEach(function () {
    buf = fs.readFileSync(__dirname + '/simple.arrow');
  });
  describe('schema', function () {
    var schema;
    beforeEach(function () {
      schema = arrow.loadSchema(buf);
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

  describe('data', function() {
    fields.forEach(function (field, i) {
      it('should read ' + field.type + ' vector ' + field.name, function () {
        var vectors = arrow.loadVectors(buf);
        var vector = vectors[field.name];
        assert.isDefined(vector, "vector " + field.name);
        assert.lengthOf(vector, field.data.length, "vector " + field.name)
        for (i = 0; i < vector.length; i += 1|0) {
          if (field.type == "Date") {
            assert.equal(vector.get(i).getTime(), field.data[i].getTime(),
                         "vector " + field.name + " index " + i);
          } else {
            assert.deepEqual(vector.get(i), field.data[i],
                             "vector " + field.name + " index " + i);
          }
        }
      });
    });
  });
});
