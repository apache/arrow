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

module Arrow
  class TableSaver
    class << self
      def save(table, path, options={})
        new(table, path, options).save
      end
    end

    def initialize(table, path, options={})
      @table = table
      @path = path
      @options = options
    end

    def save
      path = @path
      path = path.to_path if path.respond_to?(:to_path)
      format = @options[:format] || guess_format(path) || :arrow

      custom_save_method = "save_as_#{format}"
      unless respond_to?(custom_save_method, true)
        available_formats = []
        (methods(true) | private_methods(true)).each do |name|
          match_data = /\Asave_as_/.match(name.to_s)
          if match_data
            available_formats << match_data.post_match
          end
        end
        message = "Arrow::Table save format must be one of ["
        message << available_formats.join(", ")
        message << "]: #{format.inspect}"
        raise ArgumentError, message
      end
      __send__(custom_save_method, path)
    end

    private
    def guess_format(path)
      extension = ::File.extname(path).gsub(/\A\./, "").downcase
      return nil if extension.empty?

      return extension if respond_to?("save_as_#{extension}", true)

      nil
    end

    def save_raw(writer_class, path)
      FileOutputStream.open(path, false) do |output|
        writer_class.open(output, @table.schema) do |writer|
          writer.write_table(@table)
        end
      end
    end

    def save_as_arrow(path)
      save_as_batch(path)
    end

    def save_as_batch(path)
      save_raw(RecordBatchFileWriter, path)
    end

    def save_as_stream(path)
      save_raw(RecordBatchStreamWriter, path)
    end

    def save_as_csv(path)
      CSV.open(path, "w") do |csv|
        names = @table.schema.fields.collect(&:name)
        csv << names
        @table.each_record(reuse_record: true) do |record|
          csv << names.collect do |name|
            record[name]
          end
        end
      end
    end
  end
end
