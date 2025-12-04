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

class TestExtractRegexSpanOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::ExtractRegexSpanOptions.new
  end

  def test_pattern_property
    assert_equal("", @options.pattern)
    @options.pattern = "(?P<year>\\d{4})-(?P<month>\\d{2})"
    assert_equal("(?P<year>\\d{4})-(?P<month>\\d{2})", @options.pattern)
  end

  def test_extract_regex_span_function
    omit("RE2 is not available") unless Arrow::Function.find("extract_regex_span")
    args = [
      Arrow::ArrayDatum.new(build_string_array(["2023-01-15", "2024-12-31"])),
    ]
    @options.pattern = "(?P<year>\\d{4})-(?P<month>\\d{2})-(?P<day>\\d{2})"
    extract_regex_span_function = Arrow::Function.find("extract_regex_span")
    result = extract_regex_span_function.execute(args, @options).value
    fields = [
      Arrow::Field.new("year", Arrow::FixedSizeListDataType.new(Arrow::Int32DataType.new, 2)),
      Arrow::Field.new("month", Arrow::FixedSizeListDataType.new(Arrow::Int32DataType.new, 2)),
      Arrow::Field.new("day", Arrow::FixedSizeListDataType.new(Arrow::Int32DataType.new, 2)),
    ]
    assert_equal(Arrow::StructDataType.new(fields),
                 result.value_data_type)
    # The result contains [index, length] pairs for each capture group
    # year: [0, 4] (starts at index 0, length 4)
    # month: [5, 2] (starts at index 5, length 2)
    # day: [8, 2] (starts at index 8, length 2)
    assert_equal(build_struct_array(fields, [
                   {
                     "year" => [0, 4],
                     "month" => [5, 2],
                     "day" => [8, 2],
                   },
                   {
                     "year" => [0, 4],
                     "month" => [5, 2],
                     "day" => [8, 2],
                   },
                 ]),
                 result)
  end
end

