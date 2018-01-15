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

class TestDictionaryEncode < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def test_int32
    array = build_int32_array([1, 3, 1, -1, -3, -1])
    assert_equal(<<-STRING.chomp, array.dictionary_encode.to_s)

-- is_valid: all not null
-- dictionary: [1, 3, -1, -3]
-- indices: [0, 1, 0, 2, 3, 2]
    STRING
  end

  def test_string
    array = build_string_array(["Ruby", "Python", "Ruby"])
    assert_equal(<<-STRING.chomp, array.dictionary_encode.to_s)

-- is_valid: all not null
-- dictionary: ["Ruby", "Python"]
-- indices: [0, 1, 0]
    STRING
  end
end
