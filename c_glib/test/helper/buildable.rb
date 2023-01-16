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
  module Buildable
    def build_schema(fields)
      fields = fields.collect do |name, data_type|
        Arrow::Field.new(name, data_type)
      end
      Arrow::Schema.new(fields)
    end

    def build_null_array(values)
      build_array(Arrow::NullArrayBuilder.new, values)
    end

    def build_boolean_array(values)
      build_array(Arrow::BooleanArrayBuilder.new, values)
    end

    def build_int_array(values)
      build_array(Arrow::IntArrayBuilder.new, values)
    end

    def build_uint_array(values)
      build_array(Arrow::UIntArrayBuilder.new, values)
    end

    def build_int8_array(values)
      build_array(Arrow::Int8ArrayBuilder.new, values)
    end

    def build_uint8_array(values)
      build_array(Arrow::UInt8ArrayBuilder.new, values)
    end

    def build_int16_array(values)
      build_array(Arrow::Int16ArrayBuilder.new, values)
    end

    def build_uint16_array(values)
      build_array(Arrow::UInt16ArrayBuilder.new, values)
    end

    def build_int32_array(values)
      build_array(Arrow::Int32ArrayBuilder.new, values)
    end

    def build_uint32_array(values)
      build_array(Arrow::UInt32ArrayBuilder.new, values)
    end

    def build_int64_array(values)
      build_array(Arrow::Int64ArrayBuilder.new, values)
    end

    def build_uint64_array(values)
      build_array(Arrow::UInt64ArrayBuilder.new, values)
    end

    def build_half_float_array(values)
      build_array(Arrow::HalfFloatArrayBuilder.new, values)
    end

    def build_float_array(values)
      build_array(Arrow::FloatArrayBuilder.new, values)
    end

    def build_double_array(values)
      build_array(Arrow::DoubleArrayBuilder.new, values)
    end

    def build_date32_array(values)
      build_array(Arrow::Date32ArrayBuilder.new, values)
    end

    def build_date64_array(values)
      build_array(Arrow::Date64ArrayBuilder.new, values)
    end

    def build_timestamp_array(unit, values)
      data_type = Arrow::TimestampDataType.new(unit)
      build_array(Arrow::TimestampArrayBuilder.new(data_type),
                  values)
    end

    def build_time32_array(unit, values)
      build_array(Arrow::Time32ArrayBuilder.new(Arrow::Time32DataType.new(unit)),
                  values)
    end

    def build_time64_array(unit, values)
      build_array(Arrow::Time64ArrayBuilder.new(Arrow::Time64DataType.new(unit)),
                  values)
    end

    def build_binary_array(values)
      build_array(Arrow::BinaryArrayBuilder.new, values)
    end

    def build_large_binary_array(values)
      build_array(Arrow::LargeBinaryArrayBuilder.new, values)
    end

    def build_fixed_size_binary_array(data_type, values)
      build_array(Arrow::FixedSizeBinaryArrayBuilder.new(data_type),
                  values)
    end

    def build_string_array(values)
      build_array(Arrow::StringArrayBuilder.new, values)
    end

    def build_large_string_array(values)
      build_array(Arrow::LargeStringArrayBuilder.new, values)
    end

    def build_decimal128_array(value_data_type, values)
      values = values.collect do |value|
        case value
        when String
          Arrow::Decimal128.new(value)
        else
          value
        end
      end
      build_array(Arrow::Decimal128ArrayBuilder.new(value_data_type),
                  values)
    end

    def build_list_array(value_data_type, values_list, field_name: "value")
      value_field = Arrow::Field.new(field_name, value_data_type)
      data_type = Arrow::ListDataType.new(value_field)
      builder = Arrow::ListArrayBuilder.new(data_type)
      values_list.each do |values|
        append_to_builder(builder, values)
      end
      builder.finish
    end

    def build_large_list_array(value_data_type, values_list, field_name: "value")
      value_field = Arrow::Field.new(field_name, value_data_type)
      data_type = Arrow::LargeListDataType.new(value_field)
      builder = Arrow::LargeListArrayBuilder.new(data_type)
      values_list.each do |values|
        append_to_builder(builder, values)
      end
      builder.finish
    end

    def build_map_array(key_data_type, item_data_type, maps)
      data_type = Arrow::MapDataType.new(key_data_type, item_data_type)
      builder = Arrow::MapArrayBuilder.new(data_type)
      maps.each do |map|
        append_to_builder(builder, map)
      end
      builder.finish
    end

    def build_struct_array(fields, structs)
      data_type = Arrow::StructDataType.new(fields)
      builder = Arrow::StructArrayBuilder.new(data_type)
      structs.each do |struct|
        append_to_builder(builder, struct)
      end
      builder.finish
    end

    def append_to_builder(builder, value)
      if value.nil?
        builder.append_null
      else
        data_type = builder.value_data_type
        case data_type
        when Arrow::MapDataType
          builder.append_value
          key_builder = builder.key_builder
          item_builder = builder.item_builder
          value.each do |k, v|
            append_to_builder(key_builder, k)
            append_to_builder(item_builder, v)
          end
        when Arrow::ListDataType, Arrow::LargeListDataType
          builder.append_value
          value_builder = builder.value_builder
          value.each do |v|
            append_to_builder(value_builder, v)
          end
        when Arrow::StructDataType
          builder.append_value
          value.each do |name, v|
            field_index = data_type.get_field_index(name)
            field_builder = builder.get_field_builder(field_index)
            append_to_builder(field_builder, v)
          end
        else
          builder.append_value(value)
        end
      end
    end

    def build_table(columns)
      fields = []
      chunked_arrays = []
      columns.each do |name, data|
        case data
        when Arrow::Array
          chunked_array = Arrow::ChunkedArray.new([data])
        when Array
          chunked_array = Arrow::ChunkedArray.new(data)
        else
          chunked_array = data
        end
        fields << Arrow::Field.new(name, chunked_array.value_data_type)
        chunked_arrays << chunked_array
      end
      schema = Arrow::Schema.new(fields)
      Arrow::Table.new(schema, chunked_arrays)
    end

    def build_record_batch(columns)
      n_rows = columns.collect {|_, array| array.length}.min || 0
      fields = columns.collect do |name, array|
        Arrow::Field.new(name, array.value_data_type)
      end
      schema = Arrow::Schema.new(fields)
      Arrow::RecordBatch.new(schema, n_rows, columns.values)
    end

    def build_file_uri(path)
      absolute_path = File.expand_path(path)
      if absolute_path.start_with?("/")
        "file://#{absolute_path}"
      else
        "file:///#{absolute_path}"
      end
    end

    private
    def build_array(builder, values)
      values.each do |value|
        if value.nil?
          builder.append_null
        elsif builder.respond_to?(:append_string)
          builder.append_string(value)
        else
          builder.append_value(value)
        end
      end
      builder.finish
    end
  end
end
