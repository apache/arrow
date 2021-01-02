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

class TestMapDataType < Test::Unit::TestCase
  def setup
    @key_type = Arrow::StringDataType.new
    @item_type = Arrow::Int16DataType.new
    @data_type = Arrow::MapDataType.new(@key_type, @item_type)
  end

  def test_type
    assert_equal(Arrow::Type::MAP, @data_type.id)
  end

  def test_name
    assert_equal("map", @data_type.name)
  end

  def test_to_s
    assert_equal("map<string, int16>", @data_type.to_s)
  end

  def test_key
    assert_equal(@key_type, @data_type.key_type)
  end

  def test_item
    assert_equal(@item_type, @data_type.item_type)
  end
end
