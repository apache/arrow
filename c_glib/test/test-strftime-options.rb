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

class TestStrftimeOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::StrftimeOptions.new
  end

  def test_format_property
    assert_equal("%Y-%m-%dT%H:%M:%S", @options.format)
    @options.format = "%Y-%m-%d"
    assert_equal("%Y-%m-%d", @options.format)
  end

  def test_locale_property
    assert_equal("C", @options.locale)
    @options.locale = "sv_SE.UTF-8"
    assert_equal("sv_SE.UTF-8", @options.locale)
  end

  def test_strftime_function
    omit("Missing tzdata on Windows") if Gem.win_platform?
    args = [
      Arrow::ArrayDatum.new(build_timestamp_array(:milli, [1504953190854])),
    ]
    @options.format = "%Y-%m-%d"
    strftime_function = Arrow::Function.find("strftime")
    assert_equal(build_string_array(["2017-09-09"]),
                 strftime_function.execute(args, @options).value)
  end
end
