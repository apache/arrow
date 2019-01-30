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
      def save(table, output, options={})
        new(table, output, options).save
      end
    end

    def initialize(table, output, options={})
      @table = table
      output = output.to_path if output.respond_to?(:to_path)
      @output = output
      @options = options
      fill_options
    end

    def save
      format = @options[:format]
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
      if method(custom_save_method).arity.zero?
        __send__(custom_save_method)
      else
        # For backward compatibility.
        __send__(custom_save_method, @output)
      end
    end

    private
    def fill_options
      if @options[:format] and @options.key?(:compression)
        return
      end

      if @output.is_a?(Buffer)
        info = {}
      else
        extension = PathExtension.new(@output)
        info = extension.extract
      end
      format = info[:format]
      @options = @options.dup
      if format and respond_to?("save_as_#{format}", true)
        @options[:format] ||= format.to_sym
      else
        @options[:format] ||= :arrow
      end
      unless @options.key?(:compression)
        @options[:compression] = info[:compression]
      end
    end

    def open_raw_output_stream(&block)
      if @output.is_a?(Buffer)
        BufferOutputStream.open(@output, &block)
      else
        FileOutputStream.open(@output, false, &block)
      end
    end

    def open_output_stream(&block)
      compression = @options[:compression]
      if compression
        codec = Codec.new(compression)
        open_raw_output_stream do |raw_output|
          CompressedOutputStream.open(codec, raw_output) do |output|
            yield(output)
          end
        end
      else
        open_raw_output_stream(&block)
      end
    end

    def save_raw(writer_class)
      open_output_stream do |output|
        writer_class.open(output, @table.schema) do |writer|
          writer.write_table(@table)
        end
      end
    end

    def save_as_arrow
      save_as_batch
    end

    def save_as_batch
      save_raw(RecordBatchFileWriter)
    end

    def save_as_stream
      save_raw(RecordBatchStreamWriter)
    end

    def save_as_csv
      open_output_stream do |output|
        csv = CSV.new(output)
        names = @table.schema.fields.collect(&:name)
        csv << names
        @table.each_record(reuse_record: true) do |record|
          csv << names.collect do |name|
            record[name]
          end
        end
      end
    end

    def save_as_feather
      open_output_stream do |output|
        FeatherFileWriter.open(output) do |writer|
          writer.write(@table)
        end
      end
    end
  end
end
