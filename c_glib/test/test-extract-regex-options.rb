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

class TestExtractRegexOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::ExtractRegexOptions.new
  end

  def test_pattern_property
    assert_equal("", @options.pattern)
    @options.pattern = "(?P<year>\\d{4})-(?P<month>\\d{2})"
    assert_equal("(?P<year>\\d{4})-(?P<month>\\d{2})", @options.pattern)
  end

  def test_extract_regex_function
    omit("RE2 is not available") unless Arrow::Function.find("extract_regex")
    args = [
      Arrow::ArrayDatum.new(build_string_array(["2023-01-15", "2024-12-31"])),
    ]
    @options.pattern = "(?P<year>\\d{4})-(?P<month>\\d{2})-(?P<day>\\d{2})"
    extract_regex_function = Arrow::Function.find("extract_regex")
    result = extract_regex_function.execute(args, @options).value
    fields = [
               Arrow::Field.new("year", Arrow::StringDataType.new),
               Arrow::Field.new("month", Arrow::StringDataType.new),
               Arrow::Field.new("day", Arrow::StringDataType.new),
             ]
    assert_equal(Arrow::StructDataType.new(fields),
                 result.value_data_type)
    assert_equal(build_struct_array(fields, [
                   {"year" => "2023", "month" => "01", "day" => "15"},
                   {"year" => "2024", "month" => "12", "day" => "31"},
                 ]),
                 result)
  end
end

