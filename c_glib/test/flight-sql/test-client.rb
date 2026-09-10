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

class TestFlightSQLClient < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def setup
    @server = nil
    omit("Arrow Flight SQL is required") unless defined?(ArrowFlightSQL)
    omit("Unstable on Windows") if Gem.win_platform?
    @server = Helper::FlightSQLServer.new
    host = "127.0.0.1"
    location = ArrowFlight::Location.new("grpc://#{host}:0")
    options = ArrowFlight::ServerOptions.new(location)
    @server.listen(options)
    @location = ArrowFlight::Location.new("grpc://#{host}:#{@server.port}")
    @client = ArrowFlight::Client.new(@location)
    @sql_client = ArrowFlightSQL::Client.new(@client)
  end

  def teardown
    return if @server.nil?
    @server.shutdown
  end

  sub_test_case("#do_get") do
    def test_success
      info = @sql_client.execute("SELECT * FROM page_view_table")
      endpoint = info.endpoints.first
      generator = Helper::FlightInfoGenerator.new
      reader = @sql_client.do_get(endpoint.ticket)
      assert_equal(generator.page_view_table,
                   reader.read_all)
    end

    def test_error
      assert_raise(Arrow::Error::Invalid) do
        @sql_client.do_get(ArrowFlight::Ticket.new("invalid"))
      end
    end
  end

  sub_test_case("#execute_update") do
    def test_success
      insert_sql = "INSERT INTO page_view_table VALUES (100, true)"
      n_changed_records = @sql_client.execute_update(insert_sql)
      assert_equal(1, n_changed_records)
    end

    def test_error
      assert_raise(Arrow::Error::Invalid) do
        @sql_client.execute_update("INSERT")
      end
    end
  end

  sub_test_case("#prepare") do
    def test_success
      insert_sql = "INSERT INTO page_view_table VALUES (?, true)"
      statement = @sql_client.prepare(insert_sql)
      begin
        assert_equal([
                       build_schema(count: :uint64, private: :boolean),
                       build_schema(count: :uint64),
                     ],
                     [
                       statement.dataset_schema,
                       statement.parameter_schema,
                     ])
        parameters = build_record_batch(count: build_uint64_array([1, 2, 3]))
        statement.set_record_batch(parameters)
        assert_equal(3, statement.execute_update)
      ensure
        statement.close
      end
    end

    def test_error
      assert_raise(Arrow::Error::Invalid) do
        @sql_client.prepare("INSERT")
      end
    end
  end
end
