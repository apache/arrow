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

class TestListDataType < Test::Unit::TestCase
  def setup
    @field_data_type = Arrow::BooleanDataType.new
    @field = Arrow::Field.new("enabled", @field_data_type)
    @data_type = Arrow::ListDataType.new(@field)
  end

  def test_type
    assert_equal(Arrow::Type::LIST, @data_type.id)
  end

  def test_to_s
    assert_equal("list<enabled: bool>", @data_type.to_s)
  end

  def test_value_field
    assert_equal([
                   @field,
                   @field_data_type,
                 ],
                 [
                   @data_type.value_field,
                   @data_type.value_field.data_type,
                 ])
  end
end
