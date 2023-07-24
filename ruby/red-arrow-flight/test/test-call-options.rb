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

class TestCallOptions < Test::Unit::TestCase
  sub_test_case(".try_convert") do
    def test_headers
      options = ArrowFlight::CallOptions.try_convert(headers: {"a" => "b"})
      assert_equal([["a", "b"]],
                   options.headers)
    end
  end

  def setup
    @options = ArrowFlight::CallOptions.new
  end

  def test_add_header
    @options.add_header("name1", "value1")
    @options.add_header("name2", "value2")
    assert_equal([
                   ["name1", "value1"],
                   ["name2", "value2"],
                 ],
                 @options.headers)
  end

  def test_set_headers
    @options.add_header("name1", "value1")
    @options.headers = {
      "name2" => "value2",
      "name3" => "value3",
    }
    assert_equal([
                   ["name2", "value2"],
                   ["name3", "value3"],
                 ],
                 @options.headers)
  end
end
