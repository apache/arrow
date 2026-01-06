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

class TestAssumeTimezoneOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::AssumeTimezoneOptions.new
  end

  def test_timezone_property
    assert_equal("UTC", @options.timezone)
    @options.timezone = "America/New_York"
    assert_equal("America/New_York", @options.timezone)
  end

  def test_ambiguous_property
    assert_equal(Arrow::AssumeTimezoneAmbiguous::RAISE, @options.ambiguous)
    @options.ambiguous = :earliest
    assert_equal(Arrow::AssumeTimezoneAmbiguous::EARLIEST, @options.ambiguous)
    @options.ambiguous = :latest
    assert_equal(Arrow::AssumeTimezoneAmbiguous::LATEST, @options.ambiguous)
  end

  def test_nonexistent_property
    assert_equal(Arrow::AssumeTimezoneNonexistent::RAISE, @options.nonexistent)
    @options.nonexistent = :earliest
    assert_equal(Arrow::AssumeTimezoneNonexistent::EARLIEST, @options.nonexistent)
    @options.nonexistent = :latest
    assert_equal(Arrow::AssumeTimezoneNonexistent::LATEST, @options.nonexistent)
  end

  def test_assume_timezone_function
    omit("std::chrono not available on Windows MinGW") if Gem.win_platform?
    args = [
      Arrow::ArrayDatum.new(build_timestamp_array(:milli, [1504953190000])),
    ]
    @options.timezone = "America/New_York"
    @options.ambiguous = :earliest
    @options.nonexistent = :earliest
    assume_timezone_function = Arrow::Function.find("assume_timezone")
    result = assume_timezone_function.execute(args, @options).value
    assert_equal(Arrow::TimestampDataType.new(:milli, "America/New_York"),
                 result.value_data_type)
  end
end
