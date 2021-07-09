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

class TestFlightEndpoint < Test::Unit::TestCase
  def setup
    omit("Arrow Flight is required") unless defined?(ArrowFlight)
  end

  def test_ticket
    ticket = ArrowFlight::Ticket.new("data")
    locations = [
       ArrowFlight::Location.new("grpc://127.0.0.1:2929"),
       ArrowFlight::Location.new("grpc+tcp://127.0.0.1:12929"),
    ]
    endpoint = ArrowFlight::Endpoint.new(ticket, locations)
    assert_equal(ticket,
                 endpoint.ticket)
  end

  def test_locations
    ticket = ArrowFlight::Ticket.new("data")
    locations = [
       ArrowFlight::Location.new("grpc://127.0.0.1:2929"),
       ArrowFlight::Location.new("grpc+tcp://127.0.0.1:12929"),
    ]
    endpoint = ArrowFlight::Endpoint.new(ticket, locations)
    assert_equal(locations,
                 endpoint.locations)
  end

  sub_test_case("#==") do
    def test_true
      ticket = ArrowFlight::Ticket.new("data")
      location = ArrowFlight::Location.new("grpc://127.0.0.1:2929")
      endpoint1 = ArrowFlight::Endpoint.new(ticket, [location])
      endpoint2 = ArrowFlight::Endpoint.new(ticket, [location])
      assert do
        endpoint1 == endpoint2
      end
    end

    def test_false
      ticket = ArrowFlight::Ticket.new("data")
      location1 = ArrowFlight::Location.new("grpc://127.0.0.1:2929")
      location2 = ArrowFlight::Location.new("grpc://127.0.0.1:1129")
      endpoint1 = ArrowFlight::Endpoint.new(ticket, [location1])
      endpoint2 = ArrowFlight::Endpoint.new(ticket, [location2])
      assert do
        not (endpoint1 == endpoint2)
      end
    end
  end
end
