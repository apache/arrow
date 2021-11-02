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

module ArrowDataset
  class FileFormat
    class << self
      def resolve(format)
        case format
        when :arrow, :arrow_file, :arrow_streaming
          IPCFileFormat.new
        when :parquet
          ParquetFileFormat.new
        when :csv
          CSVFileFormat.new
        else
          available_formats = [
            :arrow,
            :arrow_file,
            :arrow_streaming,
            :parquet,
            :csv,
          ]
          message = "Arrow::Table load format must be one of ["
          message << available_formats.join(", ")
          message << "]: #{@options[:format].inspect}"
          raise ArgumentError, message
        end
      end
    end

    alias_method :open_writer_raw, :open_writer
    def open_writer(destination, file_system, path, schema, options)
      writer = open_writer_raw(destination, file_system, path, schema, options)
      if block_given?
        begin
          yield(writer)
        ensure
          writer.finish
        end
      else
        writer
      end
    end
  end
end
