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

  def pp_values(values)
    "[\n  " + values.collect(&:inspect).join(",\n  ") + "\n]"
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
                     ["boolean1: bool", [pp_values([false, true])]],
                     ["byte1: int8", [pp_values([1, 100])]],
                     ["short1: int16", [pp_values([1024, 2048])]],
                     ["int1: int32", [pp_values([65536, 65536])]],
                     [
                       "long1: int64",
                       [pp_values([9223372036854775807, 9223372036854775807])],
                     ],
                     ["float1: float", [pp_values([1, 2])]],
                     ["double1: double", [pp_values([-15, -5])]],
                     ["bytes1: binary", ["[\n  0001020304,\n  \n]"]],
                     ["string1: string", [pp_values(["hi", "bye"])]],
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
                     ["boolean1: bool", [pp_values([false, true])]],
                     ["short1: int16", [pp_values([1024, 2048])]],
                   ],
                   dump)
    end
  end
end
