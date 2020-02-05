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

class TestField < Test::Unit::TestCase
  def test_equal
    assert_equal(Arrow::Field.new("enabled", Arrow::BooleanDataType.new),
                 Arrow::Field.new("enabled", Arrow::BooleanDataType.new))
  end

  def test_name
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    assert_equal("enabled", field.name)
  end

  def test_data_type
    data_type = Arrow::BooleanDataType.new
    field = Arrow::Field.new("enabled", data_type)
    assert_equal(data_type.to_s, field.data_type.to_s)
  end

  def test_nullable?
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    assert do
      field.nullable?
    end
  end

  def test_to_s
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    assert_equal("enabled: bool", field.to_s)
  end
end
