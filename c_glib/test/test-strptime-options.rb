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

class TestStrptimeOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::StrptimeOptions.new
  end

  def test_format_property
    assert_equal("", @options.format)
    @options.format = "%Y-%m-%d"
    assert_equal("%Y-%m-%d", @options.format)
  end

  def test_unit_property
    assert_equal(Arrow::TimeUnit::MICRO, @options.unit)
    @options.unit = :nano
    assert_equal(Arrow::TimeUnit::NANO, @options.unit)
  end

  def test_error_is_null_property
    assert do
      !@options.error_is_null?
    end
    @options.error_is_null = true
    assert do
      @options.error_is_null?
    end
  end

  def test_strptime_function
    args = [
      Arrow::ArrayDatum.new(build_string_array(["2017-09-09T10:33:10"])),
    ]
    @options.format = "%Y-%m-%dT%H:%M:%S"
    @options.unit = :milli
    strptime_function = Arrow::Function.find("strptime")
    assert_equal(build_timestamp_array(:milli, [1504953190000]),
                 strptime_function.execute(args, @options).value)
  end
end
