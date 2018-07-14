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

class ORCTest < Test::Unit::TestCase
  include Helper::Fixture

  def setup
    omit("Require Apache Arrow ORC") unless Arrow.const_defined?(:ORCFileReader)
    @orc_path = fixture_path("TestOrcFile.test1.orc")
  end

  sub_test_case("load") do
    test("default") do
      table = Arrow::Table.load(@orc_path)
      dump = table.columns.collect do |column|
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

    test(":field_indexes") do
      table = Arrow::Table.load(@orc_path, field_indexes: [1, 3])
      dump = table.columns.collect do |column|
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
end
