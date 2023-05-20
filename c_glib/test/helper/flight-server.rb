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

require_relative "flight-info-generator"

module Helper
  class FlightAuthHandler < ArrowFlight::ServerCustomAuthHandler
    type_register

    private
    def virtual_do_authenticate(context, sender, reader)
      true
    end

    def virtual_do_is_valid(context, token)
      "identity"
    end
  end

  class FlightServer < ArrowFlight::Server
    type_register

    private
    def virtual_do_list_flights(context, criteria)
      generator = FlightInfoGenerator.new
      [generator.page_view]
    end

    def virtual_do_get_flight_info(command, criteria)
      generator = FlightInfoGenerator.new
      generator.page_view
    end

    def virtual_do_do_get(context, ticket)
      generator = FlightInfoGenerator.new
      unless ticket == generator.page_view_ticket
        raise Arrow::Error::Invalid.new("invalid ticket")
      end
      table = generator.page_view_table
      reader = Arrow::TableBatchReader.new(table)
      ArrowFlight::RecordBatchStream.new(reader)
    end
  end
end
