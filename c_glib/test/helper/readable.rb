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
  module Readable
    def read_table(input, type: :file)
      if input.is_a?(Arrow::Buffer)
        input_stream = Arrow::BufferIntputStream.new(input)
      else
        input_stream = Arrow::FileInputStream.new(input)
      end
      begin
        if type == :file
          reader = Arrow::RecordBatchFileReader.new(input_stream)
          record_batches = []
          reader.n_record_batches.times do |i|
            record_batches << reader.read_record_batch(i)
          end
          yield(Arrow::Table.new(record_batches[0].schema, record_batches))
        else
          reader = Arrow::RecordBatchStreamReader.new(input_stream)
          begin
            yield(reader.read_all)
          ensure
            reader.close
          end
        end
      ensure
        input_stream.close
      end
    end
  end
end
