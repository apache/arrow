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

require_relative "info-generator"

module Helper
  class Server < ArrowFlightSQL::Server
    type_register

    private
    def virtual_do_get_flight_info_statement(context, command, descriptor)
      generator = InfoGenerator.new
      @current_query = command.query
      handle = generate_handle(@current_query)
      generator.page_view(handle)
    end

    def virtual_do_do_get_statement(context, command)
      unless command.handle.to_s == @current_query
        raise Arrow::Error::Invalid.new("invalid ticket")
      end
      generator = InfoGenerator.new
      table = generator.page_view_table
      ArrowFlight::RecordBatchStream.new(table)
    end
  end
end
