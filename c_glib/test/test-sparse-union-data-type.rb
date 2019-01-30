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

class TestSparseUnionDataType < Test::Unit::TestCase
  def setup
    @number_field_data_type = Arrow::Int32DataType.new
    @text_field_data_type = Arrow::StringDataType.new
    @field_data_types = [
      @number_field_data_type,
      @text_field_data_type,
    ]
    @number_field = Arrow::Field.new("number", @number_field_data_type)
    @text_field = Arrow::Field.new("text", @text_field_data_type)
    @fields = [
      @number_field,
      @text_field,
    ]
    @data_type = Arrow::SparseUnionDataType.new(@fields, [2, 9])
  end

  def test_type
    assert_equal(Arrow::Type::UNION, @data_type.id)
  end

  def test_to_s
    assert_equal("union[sparse]<number: int32=2, text: string=9>",
                 @data_type.to_s)
  end

  def test_fields
    assert_equal(@fields.zip(@field_data_types),
                 @data_type.fields.collect {|field| [field, field.data_type]})
  end

  def test_get_field
    field = @data_type.get_field(0)
    assert_equal([
                   @fields[0],
                   @field_data_types[0],
                 ],
                 [
                   field,
                   field.data_type,
                 ])
  end
end
