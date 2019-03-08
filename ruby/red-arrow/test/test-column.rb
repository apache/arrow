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

class ColumnTest < Test::Unit::TestCase
  test("#each") do
    arrays = [
      Arrow::BooleanArray.new([true, false]),
      Arrow::BooleanArray.new([nil, true]),
    ]
    chunked_array = Arrow::ChunkedArray.new(arrays)
    column = Arrow::Column.new(Arrow::Field.new("visible", :boolean),
                               chunked_array)
    assert_equal([true, false, nil, true],
                 column.to_a)
  end

  test("#pack") do
    arrays = [
      Arrow::BooleanArray.new([true, false]),
      Arrow::BooleanArray.new([nil, true]),
    ]
    chunked_array = Arrow::ChunkedArray.new(arrays)
    column = Arrow::Column.new(Arrow::Field.new("visible", :boolean),
                               chunked_array)
    packed_column = column.pack
    assert_equal([1, [true, false, nil, true]],
                 [packed_column.data.n_chunks, packed_column.to_a])
  end

  sub_test_case("#==") do
    def setup
      arrays = [
        Arrow::BooleanArray.new([true]),
        Arrow::BooleanArray.new([false, true]),
      ]
      chunked_array = Arrow::ChunkedArray.new(arrays)
      @column = Arrow::Column.new(Arrow::Field.new("visible", :boolean),
                                  chunked_array)
    end

    test("Arrow::Column") do
      assert do
        @column == @column
      end
    end

    test("not Arrow::Column") do
      assert do
        not (@column == 29)
      end
    end
  end
end
