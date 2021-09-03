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


module Helper
  class InfoGenerator
    def page_view_table
      Arrow::Table.new("count" => Arrow::UInt64Array.new([1, 2, 3]),
                       "private" => Arrow::BooleanArray.new([true, false, true]))
    end

    def page_view_descriptor
      ArrowFlight::PathDescriptor.new(["page-view"])
    end

    def page_view_ticket
      "page-view"
    end

    def page_view_endpoints
      locations = [
        ArrowFlight::Location.new("grpc+tcp://127.0.0.1:10000"),
        ArrowFlight::Location.new("grpc+tcp://127.0.0.1:10001"),
      ]
      [
        ArrowFlight::Endpoint.new(page_view_ticket, locations),
      ]
    end

    def page_view
      table = page_view_table
      descriptor = page_view_descriptor
      endpoints = page_view_endpoints
      output = Arrow::ResizableBuffer.new(0)
      table.save(output, format: :stream)
      ArrowFlight::Info.new(table.schema,
                            descriptor,
                            endpoints,
                            table.n_rows,
                            output.size)
    end
  end
end
