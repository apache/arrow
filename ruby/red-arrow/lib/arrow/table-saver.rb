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
      if @output.is_a?(URI)
        custom_save_method = "save_to_uri"
      else
        custom_save_method = "save_to_file"
      end
      unless respond_to?(custom_save_method, true)
        available_schemes = []
        (methods(true) | private_methods(true)).each do |name|
          match_data = /\Asave_to_/.match(name.to_s)
          if match_data
            available_schemes << match_data.post_match
          end
        end
        message = "Arrow::Table save source must be one of ["
        message << available_schemes.join(", ")
        message << "]: #{@output.scheme.inspect}"
        raise ArgumentError, message
      end
      __send__(custom_save_method)
      @table
    end

    private
    def save_to_file
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
        deprecated_formats = ["batch", "stream"]
        available_formats -= deprecated_formats
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

    def fill_options
      if @options[:format] and @options.key?(:compression)
        return
      end

      case @output
      when Buffer
        info = {}
      when URI
        extension = PathExtension.new(@output.path)
        info = extension.extract
      else
        extension = PathExtension.new(@output)
        info = extension.extract
      end
      format = info[:format]
      @options = @options.dup
      if format
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
      save_as_arrow_file
    end

    # @since 1.0.0
    def save_as_arrow_file
      save_raw(RecordBatchFileWriter)
    end

    # @deprecated Use `format: :arrow_batch` instead.
    def save_as_batch
      save_as_arrow_file
    end

    # @since 7.0.0
    def save_as_arrows
      save_raw(RecordBatchStreamWriter)
    end

    # @since 1.0.0
    def save_as_arrow_streaming
      save_as_arrows
    end

    # @deprecated Use `format: :arrow_streaming` instead.
    def save_as_stream
      save_as_arrows
    end

    def csv_save(**options)
      open_output_stream do |output|
        csv = CSV.new(output, **options)
        names = @table.schema.fields.collect(&:name)
        csv << names
        @table.raw_records.each do |record|
          csv << record
        end
      end
    end

    def save_as_csv
      csv_save
    end

    def save_as_tsv
      csv_save(col_sep: "\t")
    end

    def save_as_feather
      properties = FeatherWriteProperties.new
      properties.class.properties.each do |name|
        value = @options[name.to_sym]
        next if value.nil?
        properties.__send__("#{name}=", value)
      end
      open_raw_output_stream do |output|
        @table.write_as_feather(output, properties)
      end
    end
  end
end
