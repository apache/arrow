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

class TestBooleanArray < Test::Unit::TestCase
  def test_mixed
    assert_equal([true, nil, false],
                 ArrowFormat::BooleanArray.new([true, nil, false]).to_a)
  end

  def test_no_null
    assert_equal([true, false],
                 ArrowFormat::BooleanArray.new([true, false]).to_a)
  end

  def test_more_8bits
    values = [true] * 8 + [nil, false]
    assert_equal(values,
                 ArrowFormat::BooleanArray.new(values).to_a)
  end
end
