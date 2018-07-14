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
    assert_nil(@reader.field_indexes)
    @reader.set_field_indexes([1, 3])
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
      assert_equal([
                     ["boolean1: bool", ["[false, true]"]],
                     ["byte1: int8", ["[1, 100]"]],
                     ["short1: int16", ["[1024, 2048]"]],
                     ["int1: int32", ["[65536, 65536]"]],
                     [
                       "long1: int64",
                       ["[9223372036854775807, 9223372036854775807]"],
                     ],
                     ["float1: float", ["[1, 2]"]],
                     ["double1: double", ["[-15, -5]"]],
                     ["bytes1: binary", ["[0001020304, ]"]],
                     ["string1: string", ["[\"hi\", \"bye\"]"]],
                     [
                       "middle: " +
                       "struct<list: " +
                       "list<item: struct<int1: int32, string1: string>>>",
                       [
                         <<-STRUCT.chomp

-- is_valid: all not null
-- child 0 type: list<item: struct<int1: int32, string1: string>> values: 
  -- is_valid: all not null
  -- value_offsets: [0, 2, 4]
  -- values: 
    -- is_valid: all not null
    -- child 0 type: int32 values: [1, 2, 1, 2]
    -- child 1 type: string values: ["bye", "sigh", "bye", "sigh"]
                          STRUCT
                       ]
                     ],
                     [
                       "list: list<item: struct<int1: int32, string1: string>>",
                       [
                         <<-LIST.chomp

-- is_valid: all not null
-- value_offsets: [0, 2, 5]
-- values: 
  -- is_valid: all not null
  -- child 0 type: int32 values: [3, 4, 100000000, -100000, 1234]
  -- child 1 type: string values: ["good", "bad", "cat", "in", "hat"]
                         LIST
                       ]
                     ],
                     [
                       "map: list<item: " +
                       "struct<key: string, value: " +
                       "struct<int1: int32, string1: string>>>",
                       [
                         <<-MAP.chomp

-- is_valid: all not null
-- value_offsets: [0, 0, 2]
-- values: 
  -- is_valid: all not null
  -- child 0 type: string values: ["chani", "mauddib"]
  -- child 1 type: struct<int1: int32, string1: string> values: 
    -- is_valid: all not null
    -- child 0 type: int32 values: [5, 1]
    -- child 1 type: string values: ["chani", "mauddib"]
                         MAP
                       ],
                     ],
                   ],
                   dump)
    end

    test("select fields") do
      @reader.set_field_indexes([1, 3])
      table = @reader.read_stripes
      dump = table.n_columns.times.collect do |i|
        column = table.get_column(i)
        [
          column.field.to_s,
          column.data.chunks.collect(&:to_s),
        ]
      end
      assert_equal([
                     ["boolean1: bool", ["[false, true]"]],
                     ["short1: int16", ["[1024, 2048]"]],
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
      assert_equal([
                     ["boolean1: bool", "[false, true]"],
                     ["byte1: int8", "[1, 100]"],
                     ["short1: int16", "[1024, 2048]"],
                     ["int1: int32", "[65536, 65536]"],
                     [
                       "long1: int64",
                       "[9223372036854775807, 9223372036854775807]",
                     ],
                     ["float1: float", "[1, 2]"],
                     ["double1: double", "[-15, -5]"],
                     ["bytes1: binary", "[0001020304, ]"],
                     ["string1: string", "[\"hi\", \"bye\"]"],
                     [
                       "middle: " +
                       "struct<list: " +
                       "list<item: struct<int1: int32, string1: string>>>",
                       <<-STRUCT.chomp

-- is_valid: all not null
-- child 0 type: list<item: struct<int1: int32, string1: string>> values: 
  -- is_valid: all not null
  -- value_offsets: [0, 2, 4]
  -- values: 
    -- is_valid: all not null
    -- child 0 type: int32 values: [1, 2, 1, 2]
    -- child 1 type: string values: ["bye", "sigh", "bye", "sigh"]
                        STRUCT
                     ],
                     [
                       "list: list<item: struct<int1: int32, string1: string>>",
                       <<-LIST.chomp

-- is_valid: all not null
-- value_offsets: [0, 2, 5]
-- values: 
  -- is_valid: all not null
  -- child 0 type: int32 values: [3, 4, 100000000, -100000, 1234]
  -- child 1 type: string values: ["good", "bad", "cat", "in", "hat"]
                       LIST
                     ],
                     [
                       "map: list<item: " +
                       "struct<key: string, value: " +
                       "struct<int1: int32, string1: string>>>",
                       <<-MAP.chomp

-- is_valid: all not null
-- value_offsets: [0, 0, 2]
-- values: 
  -- is_valid: all not null
  -- child 0 type: string values: ["chani", "mauddib"]
  -- child 1 type: struct<int1: int32, string1: string> values: 
    -- is_valid: all not null
    -- child 0 type: int32 values: [5, 1]
    -- child 1 type: string values: ["chani", "mauddib"]
                       MAP
                     ],
                   ],
                   dump)
    end

    test("select fields") do
      @reader.set_field_indexes([1, 3])
      record_batch = @reader.read_stripe(0)
      dump = record_batch.n_columns.times.collect do |i|
        [
          record_batch.schema.get_field(i).to_s,
          record_batch.get_column(i).to_s,
        ]
      end
      assert_equal([
                     ["boolean1: bool", "[false, true]"],
                     ["short1: int16", "[1024, 2048]"],
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
