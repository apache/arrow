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
  module ArrowTableSavable
    private
    def save_to_uri
      format = FileFormat.resolve(@options[:format])
      options = FileSystemDatasetWriteOptions.new
      options.file_write_options = format.default_write_options
      path = @output.path
      if @output.scheme.nil?
        options.file_system = Arrow::LocalFileSystem.new
      else
        options.file_system = Arrow::FileSystem.create(@output.to_s)
        # /C:/... -> C:/...
        unless File.expand_path(".").start_with?("/")
          path = path.gsub(/\A\//, "")
        end
      end
      partitioning = @options[:partitioning]
      if partitioning
        # TODO
        options.base_dir = File.dirname(path)
        options.base_name_template = File.basename(path)
        options.partitioning = Partitioning.resolve(@options[:partitioning])
        scanner_builder = ScannerBuilder.new(@table)
        scanner_builder.use_async(true)
        scanner = scanner_builder.finish
        FileSystemDataset.write_scanner(scanner, options)
      else
        dir = File.dirname(path)
        unless File.exist?(dir)
          options.file_system.create_dir(dir, true)
        end
        options.file_system.open_output_stream(path) do |output_stream|
          format.open_writer(output_stream,
                             options.file_system,
                             path,
                             @table.schema,
                             format.default_write_options) do |writer|
            reader = Arrow::TableBatchReader.new(@table)
            writer.write_record_batch_reader(reader)
          end
        end
      end
    end
  end
end

module Arrow
  class TableSaver
    include ArrowDataset::ArrowTableSavable
  end
end
