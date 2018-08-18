# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

class TestORCFileReader < Test::Unit::TestCase
  include Helper::Omittable
  include Helper::Fixture

  def setup
    omit("Require Apache Arrow ORC") unless Arrow.const_defined?(:ORCFileReader)
    path = fixture_path("TestOrcFile.test1.orc")
    input = Arrow::MemoryMappedInputStream.new(path)
    @reader = Arrow::ORCFileReader.new(input)
  end

  def test_read_type
    assert_equal(<<-SCHEMA.chomp, @reader.read_type.to_s)
boolean1: bool
byte1: int8
short1: int16
int1: int32
long1: int64
float1: float
double1: double
bytes1: binary
string1: string
middle: struct<list: list<item: struct<int1: int32, string1: string>>>
list: list<item: struct<int1: int32, string1: string>>
map: list<item: struct<key: string, value: struct<int1: int32, string1: string>>>
    SCHEMA
  end

  def test_field_indexes
    require_gi(1, 42, 0)
    require_gi_bindings(3, 2, 6)
    assert_nil(@reader.field_indexes)
    @reader.field_indexes = [1, 3]
    assert_equal([1, 3], @reader.field_indexes)
  end

  sub_test_case("#read_stripes") do
    test("all") do
      table = @reader.read_stripes
      dump = table.n_columns.times.collect do |i|
        column = table.get_column(i)
        [
          column.field.to_s,
          column.data.chunks.collect(&:to_s),
        ]
      end
      expected = [
                     ["boolean1: bool", ["[\n  false,\n  true\n]"]],
                     ["byte1: int8", ["[\n  1,\n  100\n]"]],
                     ["short1: int16", ["[\n  1024,\n  2048\n]"]],
                     ["int1: int32", ["[\n  65536,\n  65536\n]"]],
                     [
                       "long1: int64",
                       ["[\n  9223372036854775807,\n  9223372036854775807\n]"],
                     ],
                     ["float1: float", ["[\n  1,\n  2\n]"]],
                     ["double1: double", ["[\n  -15,\n  -5\n]"]],
                     ["bytes1: binary", ["[\n  0001020304,\n  \n]"]],
                     ["string1: string", ["[\n  \"hi\",\n  \"bye\"\n]"]],
                     [
                       "middle: " +
                       "struct<list: " +
                       "list<item: struct<int1: int32, string1: string>>>",
                       [
                         <<-STRUCT.chomp
-- is_valid: all not null
-- child 0 type: list<item: struct<int1: int32, string1: string>>
  [
    -- is_valid: all not null
    -- child 0 type: int32
      [
        1,
        2
      ]
    -- child 1 type: string
      [
        "bye",
        "sigh"
      ],
    -- is_valid: all not null
    -- child 0 type: int32
      [
        1,
        2
      ]
    -- child 1 type: string
      [
        "bye",
        "sigh"
      ]
  ]
                          STRUCT
                       ]
                     ],
                     [
                       "list: list<item: struct<int1: int32, string1: string>>",
                       [
                         <<-LIST.chomp
[
  -- is_valid: all not null
  -- child 0 type: int32
    [
      3,
      4
    ]
  -- child 1 type: string
    [
      "good",
      "bad"
    ],
  -- is_valid: all not null
  -- child 0 type: int32
    [
      100000000,
      -100000,
      1234
    ]
  -- child 1 type: string
    [
      "cat",
      "in",
      "hat"
    ]
]
                         LIST
                       ]
                     ],
                     [
                       "map: list<item: " +
                       "struct<key: string, value: " +
                       "struct<int1: int32, string1: string>>>",
                       [
                         <<-MAP.chomp
[
  -- is_valid: all not null
  -- child 0 type: string
    []
  -- child 1 type: struct<int1: int32, string1: string>
    -- is_valid: all not null
    -- child 0 type: int32
      []
    -- child 1 type: string
      [],
  -- is_valid: all not null
  -- child 0 type: string
    [
      "chani",
      "mauddib"
    ]
  -- child 1 type: struct<int1: int32, string1: string>
    -- is_valid: all not null
    -- child 0 type: int32
      [
        5,
        1
      ]
    -- child 1 type: string
      [
        "chani",
        "mauddib"
      ]
]
                         MAP
                       ],
                     ],
                   ]
      expected.zip(dump).each do |ex, actual|
        assert_equal(ex, actual)
      end
    end

    test("select fields") do
      require_gi_bindings(3, 2, 6)
      @reader.field_indexes = [1, 3]
      table = @reader.read_stripes
      dump = table.n_columns.times.collect do |i|
        column = table.get_column(i)
        [
          column.field.to_s,
          column.data.chunks.collect(&:to_s),
        ]
      end
      assert_equal([
                     ["boolean1: bool", ["[\n  false,\n  true\n]"]],
                     ["short1: int16", ["[\n  1024,\n  2048\n]"]],
                   ],
                   dump)
    end
  end

  sub_test_case("#read_stripe") do
    test("all") do
      record_batch = @reader.read_stripe(0)
      dump = record_batch.n_columns.times.collect do |i|
        [
          record_batch.schema.get_field(i).to_s,
          record_batch.get_column(i).to_s,
        ]
      end
      expected = [
                     ["boolean1: bool", "[\n  false,\n  true\n]"],
                     ["byte1: int8", "[\n  1,\n  100\n]"],
                     ["short1: int16", "[\n  1024,\n  2048\n]"],
                     ["int1: int32", "[\n  65536,\n  65536\n]"],
                     [
                       "long1: int64",
                       "[\n  9223372036854775807,\n  9223372036854775807\n]",
                     ],
                     ["float1: float", "[\n  1,\n  2\n]"],
                     ["double1: double", "[\n  -15,\n  -5\n]"],
                     ["bytes1: binary", "[\n  0001020304,\n  \n]"],
                     ["string1: string", "[\n  \"hi\",\n  \"bye\"\n]"],
                     [
                       "middle: " +
                       "struct<list: " +
                       "list<item: struct<int1: int32, string1: string>>>",
                       <<-STRUCT.chomp
-- is_valid: all not null
-- child 0 type: list<item: struct<int1: int32, string1: string>>
  [
    -- is_valid: all not null
    -- child 0 type: int32
      [
        1,
        2
      ]
    -- child 1 type: string
      [
        "bye",
        "sigh"
      ],
    -- is_valid: all not null
    -- child 0 type: int32
      [
        1,
        2
      ]
    -- child 1 type: string
      [
        "bye",
        "sigh"
      ]
  ]
                        STRUCT
                     ],
                     [
                       "list: list<item: struct<int1: int32, string1: string>>",
                       <<-LIST.chomp
[
  -- is_valid: all not null
  -- child 0 type: int32
    [
      3,
      4
    ]
  -- child 1 type: string
    [
      "good",
      "bad"
    ],
  -- is_valid: all not null
  -- child 0 type: int32
    [
      100000000,
      -100000,
      1234
    ]
  -- child 1 type: string
    [
      "cat",
      "in",
      "hat"
    ]
]
                       LIST
                     ],
                     [
                       "map: list<item: " +
                       "struct<key: string, value: " +
                       "struct<int1: int32, string1: string>>>",
                       <<-MAP.chomp
[
  -- is_valid: all not null
  -- child 0 type: string
    []
  -- child 1 type: struct<int1: int32, string1: string>
    -- is_valid: all not null
    -- child 0 type: int32
      []
    -- child 1 type: string
      [],
  -- is_valid: all not null
  -- child 0 type: string
    [
      "chani",
      "mauddib"
    ]
  -- child 1 type: struct<int1: int32, string1: string>
    -- is_valid: all not null
    -- child 0 type: int32
      [
        5,
        1
      ]
    -- child 1 type: string
      [
        "chani",
        "mauddib"
      ]
]
                       MAP
                     ],
                   ]
      expected.zip(dump).each do |ex, actual|
        assert_equal(ex, actual)
      end
    end

    test("select fields") do
      require_gi_bindings(3, 2, 6)
      @reader.field_indexes = [1, 3]
      record_batch = @reader.read_stripe(0)
      dump = record_batch.n_columns.times.collect do |i|
        [
          record_batch.schema.get_field(i).to_s,
          record_batch.get_column(i).to_s,
        ]
      end
      assert_equal([
                     ["boolean1: bool", "[\n  false,\n  true\n]"],
                     ["short1: int16", "[\n  1024,\n  2048\n]"],
                   ],
                   dump)
    end
  end

  def test_n_stripes
    assert_equal(1, @reader.n_stripes)
  end

  def test_n_rows
    assert_equal(2, @reader.n_rows)
  end
end
