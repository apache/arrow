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
  class TableLoader
    class << self
      def load(path, options={})
        new(path, options).load
      end
    end

    def initialize(path, options={})
      path = path.to_path if path.respond_to?(:to_path)
      @path = path
      @options = options
      fill_options
    end

    def load
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
        message = "Arrow::Table load format must be one of ["
        message << available_formats.join(", ")
        message << "]: #{format.inspect}"
        raise ArgumentError, message
      end
      if method(custom_load_method).arity.zero?
        __send__(custom_load_method)
      else
        # For backward compatibility.
        __send__(custom_load_method, @path)
      end
    end

    private
    def fill_options
      if @options[:format] and @options.key?(:compression)
        return
      end

      extension = PathExtension.new(@path)
      info = extension.extract
      format = info[:format]
      @options = @options.dup
      if respond_to?("load_as_#{format}", true)
        @options[:format] ||= format.to_sym
      else
        @options[:format] ||= :arrow
      end
      unless @options.key?(:compression)
        @options[:compression] = info[:compression]
      end
    end

    def load_raw(input, reader)
      schema = reader.schema
      chunked_arrays = []
      reader.each do |record_batch|
        record_batch.columns.each_with_index do |array, i|
          chunked_array = (chunked_arrays[i] ||= [])
          chunked_array << array
        end
      end
      columns = schema.fields.collect.with_index do |field, i|
        Column.new(field, ChunkedArray.new(chunked_arrays[i]))
      end
      table = Table.new(schema, columns)
      table.instance_variable_set(:@input, input)
      table
    end

    def load_as_arrow
      input = nil
      reader = nil
      error = nil
      reader_class_candidates = [
        RecordBatchFileReader,
        RecordBatchStreamReader,
      ]
      reader_class_candidates.each do |reader_class_candidate|
        input = MemoryMappedInputStream.new(@path)
        begin
          reader = reader_class_candidate.new(input)
        rescue Arrow::Error
          error = $!
        else
          break
        end
      end
      raise error if reader.nil?
      load_raw(input, reader)
    end

    def load_as_batch
      input = MemoryMappedInputStream.new(@path)
      reader = RecordBatchFileReader.new(input)
      load_raw(input, reader)
    end

    def load_as_stream
      input = MemoryMappedInputStream.new(@path)
      reader = RecordBatchStreamReader.new(input)
      load_raw(input, reader)
    end

    if Arrow.const_defined?(:ORCFileReader)
      def load_as_orc
        input = MemoryMappedInputStream.new(@path)
        reader = ORCFileReader.new(input)
        field_indexes = @options[:field_indexes]
        reader.set_field_indexes(field_indexes) if field_indexes
        table = reader.read_stripes
        table.instance_variable_set(:@input, input)
        table
      end
    end

    def load_as_csv
      options = @options.dup
      options.delete(:format)
      CSVLoader.load(Pathname.new(@path), options)
    end

    def load_as_feather
      input = MemoryMappedInputStream.new(@path)
      reader = FeatherFileReader.new(input)
      table = reader.read
      table.instance_variable_set(:@input, input)
      table
    end
  end
end
