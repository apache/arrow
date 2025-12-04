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

class TestPadOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::PadOptions.new
  end

  def test_width_property
    assert_equal(0, @options.width)
    @options.width = 5
    assert_equal(5, @options.width)
  end

  def test_padding_property
    assert_equal(" ", @options.padding)
    @options.padding = "0"
    assert_equal("0", @options.padding)
  end

  def test_lean_left_on_odd_padding_property
    assert do
      @options.lean_left_on_odd_padding?
    end
    @options.lean_left_on_odd_padding = false
    assert do
      not @options.lean_left_on_odd_padding?
    end
  end

  def test_utf8_center_function
    args = [
      Arrow::ArrayDatum.new(build_string_array(["a", "ab", "abc"])),
    ]
    utf8_center_function = Arrow::Function.find("utf8_center")

    @options.width = 5
    @options.padding = " "
    result = utf8_center_function.execute(args, @options).value
    assert_equal(build_string_array(["  a  ", " ab  ", " abc "]), result)

    @options.lean_left_on_odd_padding = false
    result = utf8_center_function.execute(args, @options).value
    assert_equal(build_string_array(["  a  ", "  ab ", " abc "]), result)
  end
end

