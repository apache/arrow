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

class TestReplaceSliceOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::ReplaceSliceOptions.new
  end

  def test_start_property
    assert_equal(0, @options.start)
    @options.start = 1
    assert_equal(1, @options.start)
  end

  def test_stop_property
    assert_equal(0, @options.stop)
    @options.stop = 2
    assert_equal(2, @options.stop)
  end

  def test_replacement_property
    assert_equal("", @options.replacement)
    @options.replacement = "XX"
    assert_equal("XX", @options.replacement)
  end

  def test_utf8_replace_slice_function
    args = [
      Arrow::ArrayDatum.new(build_string_array(["hello", "world"])),
    ]
    @options.start = 1
    @options.stop = 3
    @options.replacement = "XX"
    utf8_replace_slice_function = Arrow::Function.find("utf8_replace_slice")
    result = utf8_replace_slice_function.execute(args, @options).value
    expected = build_string_array(["hXXlo", "wXXld"])
    assert_equal(expected, result)
  end
end
