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

class TestTrimOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::TrimOptions.new
  end

  def test_characters_property
    assert_equal("", @options.characters)
    @options.characters = " \t"
    assert_equal(" \t", @options.characters)
  end

  def test_utf8_trim_function
    args = [
      Arrow::ArrayDatum.new(build_string_array(["  hello  ", "  world  "])),
    ]
    @options.characters = " "
    utf8_trim_function = Arrow::Function.find("utf8_trim")
    result = utf8_trim_function.execute(args, @options).value
    expected = build_string_array(["hello", "world"])
    assert_equal(expected, result)
  end
end
