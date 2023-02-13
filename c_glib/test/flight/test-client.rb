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

class TestFlightClient < Test::Unit::TestCase
  include Helper::Omittable

  def setup
    @server = nil
    omit("Arrow Flight is required") unless defined?(ArrowFlight)
    omit("Unstable on Windows") if Gem.win_platform?
    require_gi_bindings(3, 4, 7)
    @server = Helper::FlightServer.new
    host = "127.0.0.1"
    location = ArrowFlight::Location.new("grpc://#{host}:0")
    options = ArrowFlight::ServerOptions.new(location)
    options.auth_handler = Helper::FlightAuthHandler.new
    @server.listen(options)
    @location = ArrowFlight::Location.new("grpc://#{host}:#{@server.port}")
  end

  def teardown
    return if @server.nil?
    @server.shutdown
  end

  def test_close
    client = ArrowFlight::Client.new(@location)
    client.close
    # Idempotent
    client.close
  end

  def test_authenticate_basic_token
    client = ArrowFlight::Client.new(@location)
    generator = Helper::FlightInfoGenerator.new
    assert_equal([true, "", ""],
                 client.authenticate_basic_token("user", "password"))
  end

  def test_list_flights
    client = ArrowFlight::Client.new(@location)
    generator = Helper::FlightInfoGenerator.new
    assert_equal([generator.page_view],
                 client.list_flights)
  end

  def test_get_flight_info
    client = ArrowFlight::Client.new(@location)
    request = ArrowFlight::CommandDescriptor.new("page-view")
    generator = Helper::FlightInfoGenerator.new
    assert_equal(generator.page_view,
                 client.get_flight_info(request))
  end

  sub_test_case("#do_get") do
    def test_success
      client = ArrowFlight::Client.new(@location)
      info = client.list_flights.first
      endpoint = info.endpoints.first
      generator = Helper::FlightInfoGenerator.new
      reader = client.do_get(endpoint.ticket)
      assert_equal(generator.page_view_table,
                   reader.read_all)
    end

    def test_error
      client = ArrowFlight::Client.new(@location)
      assert_raise(Arrow::Error::Invalid) do
        client.do_get(ArrowFlight::Ticket.new("invalid"))
      end
    end
  end
end
