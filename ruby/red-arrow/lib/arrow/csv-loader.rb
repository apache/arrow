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

require "csv"
require "pathname"
require "time"

module Arrow
  class CSVLoader
    class << self
      def load(path_or_data, **options)
        new(path_or_data, **options).load
      end
    end

    def initialize(path_or_data, **options)
      @path_or_data = path_or_data
      @options = options
    end

    def load
      case @path_or_data
      when Pathname
        load_from_path(@path_or_data.to_path)
      when /\A.+\.csv\z/i
        load_from_path(@path_or_data)
      else
        load_data(@path_or_data)
      end
    end

    private
    def open_csv(path, **options)
      CSV.open(path, **options) do |csv|
        yield(csv)
      end
    end

    def parse_csv_data(data, **options)
      csv = CSV.new(data, **options)
      begin
        yield(csv)
      ensure
        csv.close
      end
    end

    def read_csv(csv)
      reader = CSVReader.new(csv)
      reader.read
    end

    def load_from_path(path)
      options = update_csv_parse_options(@options, :open_csv, path)
      open_csv(path, **options) do |csv|
        read_csv(csv)
      end
    end

    def load_data(data)
      options = update_csv_parse_options(@options, :parse_csv_data, data)
      parse_csv_data(data, **options) do |csv|
        read_csv(csv)
      end
    end

    def selective_converter(target_index)
      lambda do |field, field_info|
        if target_index.nil? or field_info.index == target_index
          yield(field)
        else
          field
        end
      end
    end

    BOOLEAN_CONVERTER = lambda do |field|
      begin
        encoded_field = field.encode(CSV::ConverterEncoding)
      rescue EncodingError
        field
      else
        case encoded_field
        when "true"
          true
        when "false"
          false
        else
          field
        end
      end
    end

    ISO8601_CONVERTER = lambda do |field|
      begin
        encoded_field = field.encode(CSV::ConverterEncoding)
      rescue EncodingError
        field
      else
        begin
          Time.iso8601(encoded_field)
        rescue ArgumentError
          field
        end
      end
    end

    def update_csv_parse_options(options, create_csv, *args)
      if options.key?(:converters)
        new_options = options.dup
      else
        converters = [:all, BOOLEAN_CONVERTER, ISO8601_CONVERTER]
        new_options = options.merge(converters: converters)
      end

      unless options.key?(:headers)
        __send__(create_csv, *args, **new_options) do |csv|
          new_options[:headers] = have_header?(csv)
        end
      end
      unless options.key?(:converters)
        __send__(create_csv, *args, **new_options) do |csv|
          new_options[:converters] = detect_robust_converters(csv)
        end
      end

      new_options
    end

    def have_header?(csv)
      if @options.key?(:headers)
        return @options[:headers]
      end

      row1 = csv.shift
      return false if row1.nil?
      return false if row1.any?(&:nil?)

      row2 = csv.shift
      return nil if row2.nil?
      return true if row2.any?(&:nil?)

      return false if row1.any? {|value| not value.is_a?(String)}

      if row1.collect(&:class) != row2.collect(&:class)
        return true
      end

      nil
    end

    def detect_robust_converters(csv)
      column_types = []
      csv.each do |row|
        if row.is_a?(CSV::Row)
          each_value = Enumerator.new do |yielder|
            row.each do |_name, value|
              yielder << value
            end
          end
        else
          each_value = row.each
        end
        each_value.with_index do |value, i|
          current_column_type = column_types[i]
          next if current_column_type == :string

          candidate_type = nil
          case value
          when nil
            next
          when "true", "false", true, false
            candidate_type = :boolean
          when Integer
            candidate_type = :integer
            if current_column_type == :float
              candidate_type = :float
            end
          when Float
            candidate_type = :float
            if current_column_type == :integer
              column_types[i] = candidate_type
            end
          when Time
            candidate_type = :time
          when DateTime
            candidate_type = :date_time
          when Date
            candidate_type = :date
          when String
            next if value.empty?
            candidate_type = :string
          else
            candidate_type = :string
          end

          column_types[i] ||= candidate_type
          if column_types[i] != candidate_type
            column_types[i] = :string
          end
        end
      end

      converters = []
      column_types.each_with_index do |type, i|
        case type
        when :boolean
          converters << selective_converter(i, &BOOLEAN_CONVERTER)
        when :integer
          converters << selective_converter(i) do |field|
            if field.nil? or field.empty?
              nil
            else
              CSV::Converters[:integer].call(field)
            end
          end
        when :float
          converters << selective_converter(i) do |field|
            if field.nil? or field.empty?
              nil
            else
              CSV::Converters[:float].call(field)
            end
          end
        when :time
          converters << selective_converter(i, &ISO8601_CONVERTER)
        when :date_time
          converters << selective_converter(i, &CSV::Converters[:date_time])
        when :date
          converters << selective_converter(i, &CSV::Converters[:date])
        end
      end
      converters
    end
  end
end
