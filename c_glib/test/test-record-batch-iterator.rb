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

class TestRecordBatchIterator <Test::Unit::TestCase
  include Helper::Buildable

  def setup
    fields = [
      Arrow::Field.new("visible", Arrow::BooleanDataType.new),
      Arrow::Field.new("point", Arrow::Int32DataType.new),
    ]
    schema = Arrow::Schema.new(fields)
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
      Arrow::RecordBatch.new(schema, columns[0].length, columns)
    end
    @iterator = Arrow::RecordBatchIterator.new(@record_batches)
  end

  def test_next
    assert_equal(@record_batches[0], @iterator.next)
    assert_equal(@record_batches[1], @iterator.next)
    assert_equal(nil, @iterator.next)
  end

  def test_to_list
    assert_equal(@record_batches, @iterator.to_list)
  end
end
