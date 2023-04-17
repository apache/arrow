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

require "open-uri"

module Arrow
  class TableLoader
    class << self
      def load(input, options={})
        new(input, options).load
      end
    end

    def initialize(input, options={})
      input = input.to_path if input.respond_to?(:to_path)
      @input = input
      @options = options
      fill_options
    end

    def load
      if @input.is_a?(URI)
        custom_load_method_candidates = []
        if @input.scheme
          custom_load_method_candidates << "load_from_uri_#{@input.scheme}"
        end
        custom_load_method_candidates << "load_from_uri"
      elsif @input.is_a?(String) and ::File.directory?(@input)
        custom_load_method_candidates = ["load_from_directory"]
      else
        custom_load_method_candidates = ["load_from_file"]
      end
      custom_load_method_candidates.each do |custom_load_method|
        next unless respond_to?(custom_load_method, true)
        return __send__(custom_load_method)
      end
      available_schemes = []
      (methods(true) | private_methods(true)).each do |name|
        match_data = /\Aload_from_/.match(name.to_s)
        if match_data
          available_schemes << match_data.post_match
        end
      end
      message = "Arrow::Table load source must be one of ["
      message << available_schemes.join(", ")
      message << "]: #{@input.inspect}"
      raise ArgumentError, message
    end

    private
    def load_from_uri_http
      load_by_reader
    end

    def load_from_uri_https
      load_by_reader
    end

    def load_from_file
      load_by_reader
    end

    def load_by_reader
      format = @options[:format]
      custom_load_method = "load_as_#{format}"
      unless respond_to?(custom_load_method, true)
        available_formats = []
        (methods(true) | private_methods(true)).each do |name|
          match_data = /\Aload_as_/.match(name.to_s)
          if match_data
            available_formats << match_data.post_match
          end
        end
        deprecated_formats = ["batch", "stream"]
        available_formats -= deprecated_formats
        message = "Arrow::Table load format must be one of ["
        message << available_formats.join(", ")
        message << "]: #{format.inspect}"
        raise ArgumentError, message
      end
      if method(custom_load_method).arity.zero?
        __send__(custom_load_method)
      else
        # For backward compatibility.
        __send__(custom_load_method, @input)
      end
    end

    def fill_options
      if @options[:format] and @options.key?(:compression)
        return
      end

      case @input
      when Buffer
        info = {}
      when URI
        extension = PathExtension.new(@input.path)
        info = extension.extract
      else
        extension = PathExtension.new(@input)
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

    def open_input_stream
      case @input
      when Buffer
        yield(BufferInputStream.new(@input))
      when URI
        @input.open do |ruby_input|
          case @options[:format]
          when :stream, :arrow_streaming
            Gio::RubyInputStream.open(ruby_input) do |gio_input|
              GIOInputStream.open(gio_input) do |input|
                yield(input)
              end
            end
          else
            # TODO: We need to consider Ruby's GVL carefully to use
            # Ruby object directly for input with other formats. We
            # read data and use it as Buffer for now.
            data = GLib::Bytes.new(ruby_input.read.freeze)
            buffer = Buffer.new(data)
            yield(BufferInputStream.new(buffer))
          end
        end
      else
        yield(MemoryMappedInputStream.new(@input))
      end
    end

    def load_raw(input, reader)
      schema = reader.schema
      record_batches = []
      reader.each do |record_batch|
        record_batches << record_batch
      end
      table = Table.new(schema, record_batches)
      table.refer_input(input)
      table
    end

    def load_as_arrow
      begin
        load_as_arrow_file
      rescue
        load_as_arrows
      end
    end

    # @since 1.0.0
    def load_as_arrow_file
      open_input_stream do |input|
        reader = RecordBatchFileReader.new(input)
        load_raw(input, reader)
      end
    end

    # @deprecated Use `format: :arrow_file` instead.
    def load_as_batch
      load_as_arrow_file
    end

    # @since 7.0.0
    def load_as_arrows
      open_input_stream do |input|
        reader = RecordBatchStreamReader.new(input)
        load_raw(input, reader)
      end
    end

    # @since 1.0.0
    def load_as_arrow_streaming
      load_as_arrows
    end

    # @deprecated Use `format: :arrow_streaming` instead.
    def load_as_stream
      load_as_arrows
    end

    if Arrow.const_defined?(:ORCFileReader)
      def load_as_orc
        open_input_stream do |input|
          reader = ORCFileReader.new(input)
          field_indexes = @options[:field_indexes]
          reader.set_field_indexes(field_indexes) if field_indexes
          table = reader.read_stripes
          table.refer_input(input)
          table
        end
      end
    end

    def csv_load(options)
      options.delete(:format)
      case @input
      when Buffer
        CSVLoader.load(@input.data.to_s, **options)
      when URI
        @input.open do |input|
          CSVLoader.load(input.read, **options)
        end
      else
        CSVLoader.load(Pathname.new(@input), **options)
      end
    end

    def load_as_csv
      csv_load(@options.dup)
    end

    def load_as_tsv
      options = @options.dup
      options[:delimiter] = "\t"
      csv_load(options.dup)
    end

    def load_as_feather
      open_input_stream do |input|
        reader = FeatherFileReader.new(input)
        table = reader.read
        table.refer_input(input)
        table
      end
    end

    def load_as_json
      open_input_stream do |input|
        reader = JSONReader.new(input)
        table = reader.read
        table.refer_input(input)
        table
      end
    end
  end
end
