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

class TestFlightLocation < Test::Unit::TestCase
  def setup
    omit("Arrow Flight is required") unless defined?(ArrowFlight)
  end

  def test_to_s
    location = ArrowFlight::Location.new("grpc://127.0.0.1:2929")
    assert_equal("grpc://127.0.0.1:2929", location.to_s)
  end

  def test_scheme
    location = ArrowFlight::Location.new("grpc://127.0.0.1:2929")
    assert_equal("grpc", location.scheme)
  end

  def test_equal
    location1 = ArrowFlight::Location.new("grpc://127.0.0.1:2929")
    location2 = ArrowFlight::Location.new("grpc://127.0.0.1:2929")
    assert do
      location1 == location2
    end
  end
end
