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
  class FlightSQLServer < ArrowFlightSQL::Server
    type_register

    private
    def virtual_do_get_flight_info_statement(context, command, descriptor)
      generator = FlightInfoGenerator.new
      @current_query = command.query
      handle =
        ArrowFlightSQL::StatementQueryTicket.generate_handle(@current_query)
      generator.page_view(ArrowFlight::Ticket.new(handle))
    end

    def virtual_do_do_get_statement(context, command)
      unless command.handle.to_s == @current_query
        raise Arrow::Error::Invalid.new("invalid ticket")
      end
      generator = FlightInfoGenerator.new
      table = generator.page_view_table
      reader = Arrow::TableBatchReader.new(table)
      ArrowFlight::RecordBatchStream.new(reader)
    end

    def virtual_do_do_put_command_statement_update(context, command)
      unless command.query == "INSERT INTO page_view_table VALUES (100, true)"
        raise Arrow::Error::Invalid.new("invalid SQL")
      end
      1
    end

    def virtual_do_create_prepared_statement(context, request)
      unless request.query == "INSERT INTO page_view_table VALUES (?, true)"
        raise Arrow::Error::Invalid.new("invalid SQL")
      end
      result = ArrowFlightSQL::CreatePreparedStatementResult.new
      generator = FlightInfoGenerator.new
      table = generator.page_view_table
      result.dataset_schema = table.schema
      result.parameter_schema = table.schema.remove_field(1)
      result.handle = "valid-handle"
      result
    end

    def virtual_do_do_put_prepared_statement_update(context, command, reader)
      unless command.handle.to_s == "valid-handle"
        raise Arrow::Error::Invalid.new("invalid handle")
      end
      reader.read_all.n_rows
    end

    def virtual_do_close_prepared_statement(context, request)
      unless request.handle.to_s == "valid-handle"
        raise Arrow::Error::Invalid.new("invalid handle")
      end
    end
  end
end
