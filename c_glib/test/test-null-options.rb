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

class TestNullOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::NullOptions.new
  end

  def test_nan_is_null_property
    assert do
      !@options.nan_is_null?
    end
    @options.nan_is_null = true
    assert do
      @options.nan_is_null?
    end
  end

  def test_is_null_function
    args = [
      Arrow::ArrayDatum.new(build_float_array([1.0, Float::NAN, 2.0, nil, 4.0])),
    ]
    is_null_function = Arrow::Function.find("is_null")
    @options.nan_is_null = true
    result = is_null_function.execute(args, @options).value
    assert_equal(build_boolean_array([false, true, false, true, false]), result)
  end
end
