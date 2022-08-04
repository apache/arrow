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

class TestFlightCallOptions < Test::Unit::TestCase
  def setup
    omit("Arrow Flight is required") unless defined?(ArrowFlight)
    @options = ArrowFlight::CallOptions.new
  end

  def collect_headers
    headers = []
    @options.foreach_header do |name, value|
      headers << [name, value]
    end
    headers
  end

  def test_add_headers
    @options.add_header("name1", "value1")
    @options.add_header("name2", "value2")
    assert_equal([
                   ["name1", "value1"],
                   ["name2", "value2"],
                 ],
                 collect_headers)
  end

  def test_clear_headers
    @options.add_header("name1", "value1")
    @options.clear_headers
    assert_equal([], collect_headers)
  end
end
