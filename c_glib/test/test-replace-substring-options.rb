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

class TestReplaceSubstringOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::ReplaceSubstringOptions.new
  end

  def test_pattern_property
    assert_equal("", @options.pattern)
    @options.pattern = "foo"
    assert_equal("foo", @options.pattern)
  end

  def test_replacement_property
    assert_equal("", @options.replacement)
    @options.replacement = "bar"
    assert_equal("bar", @options.replacement)
  end

  def test_max_replacements_property
    assert_equal(-1, @options.max_replacements)
    @options.max_replacements = 1
    assert_equal(1, @options.max_replacements)
  end

  def test_replace_substring_function
    args = [
      Arrow::ArrayDatum.new(build_string_array(["foo", "this foo that foo", "bar"])),
    ]
    @options.pattern = "foo"
    @options.replacement = "baz"
    replace_substring_function = Arrow::Function.find("replace_substring")
    result = replace_substring_function.execute(args, @options).value
    expected = build_string_array(["baz", "this baz that baz", "bar"])
    assert_equal(expected, result)
  end

  def test_replace_substring_with_max_replacements
    args = [
      Arrow::ArrayDatum.new(build_string_array(["this foo that foo"])),
    ]
    @options.pattern = "foo"
    @options.replacement = "baz"
    @options.max_replacements = 1
    replace_substring_function = Arrow::Function.find("replace_substring")
    result = replace_substring_function.execute(args, @options).value
    expected = build_string_array(["this baz that foo"])
    assert_equal(expected, result)
  end
end
