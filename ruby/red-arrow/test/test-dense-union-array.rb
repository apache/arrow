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

class DenseUnionArrayTest < Test::Unit::TestCase
  def setup
    data_type_fields = [
      Arrow::Field.new("number", :int16),
      Arrow::Field.new("text", :string),
    ]
    type_codes = [11, 13]
    @data_type = Arrow::DenseUnionDataType.new(data_type_fields, type_codes)
    type_ids = Arrow::Int8Array.new([11, 13, 11, 13, 13])
    value_offsets = Arrow::Int32Array.new([0, 0, 1, 1, 2])
    fields = [
      Arrow::Int16Array.new([1, nil]),
      Arrow::StringArray.new(["a", "b", "c"])
    ]
    @array = Arrow::DenseUnionArray.new(@data_type,
                                        type_ids,
                                        value_offsets,
                                        fields)
  end

  def test_get_value
    assert_equal([1, "a", nil, "b", "c"],
                 @array.length.times.collect {|i| @array[i]})
  end
end
