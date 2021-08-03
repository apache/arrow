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

class TestRecordBatchReader <Test::Unit::TestCase
  include Helper::Buildable

  def setup
    fields = [
      Arrow::Field.new("visible", Arrow::BooleanDataType.new),
      Arrow::Field.new("point", Arrow::Int32DataType.new),
    ]
    @schema = Arrow::Schema.new(fields)
    @record_batches = [
      [
        build_boolean_array([true, false, true]),
        build_int32_array([1, 2, 3]),
      ],
      [
        build_boolean_array([false, true, false, true]),
        build_int32_array([-1, -2, -3, -4]),
      ]
    ].collect do |columns|
      Arrow::RecordBatch.new(@schema, columns[0].length, columns)
    end
    @reader = Arrow::RecordBatchReader.new(@record_batches, @schema)
  end

  def test_schema
    assert_equal(@schema, @reader.schema)
  end

  def test_read_next
    assert_equal(@record_batches[0], @reader.read_next)
    assert_equal(@record_batches[1], @reader.read_next)
    assert_nil(@reader.read_next)
  end

  def test_read_all
    assert_equal(Arrow::Table.new(@schema, @record_batches),
                 @reader.read_all)
  end
end
