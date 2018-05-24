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

class ChunkedArrayTest < Test::Unit::TestCase
  test("#each") do
    arrays = [
      Arrow::BooleanArray.new([true, false]),
      Arrow::BooleanArray.new([nil, true]),
    ]
    chunked_array = Arrow::ChunkedArray.new(arrays)
    assert_equal([true, false, nil, true],
                 chunked_array.to_a)
  end

  sub_test_case("#pack") do
    test("basic array") do
      arrays = [
        Arrow::BooleanArray.new([true, false]),
        Arrow::BooleanArray.new([nil, true]),
      ]
      chunked_array = Arrow::ChunkedArray.new(arrays)
      packed_chunked_array = chunked_array.pack
      assert_equal([
                     Arrow::BooleanArray,
                     [true, false, nil, true],
                   ],
                   [
                     packed_chunked_array.class,
                     packed_chunked_array.to_a,
                   ])
    end

    test("TimestampArray") do
      type = Arrow::TimestampDataType.new(:nano)
      arrays = [
        Arrow::TimestampArrayBuilder.new(type).build([Time.at(0)]),
        Arrow::TimestampArrayBuilder.new(type).build([Time.at(1)]),
      ]
      chunked_array = Arrow::ChunkedArray.new(arrays)
      packed_chunked_array = chunked_array.pack
      assert_equal([
                     Arrow::TimestampArray,
                     [Time.at(0), Time.at(1)],
                   ],
                   [
                     packed_chunked_array.class,
                     packed_chunked_array.to_a,
                   ])
    end
  end
end
