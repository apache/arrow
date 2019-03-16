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

    def build_string_array(values)
      build_array(Arrow::StringArrayBuilder.new, values)
    end

    def build_list_array(value_data_type, values_list, field_name: "value")
      value_field = Arrow::Field.new(field_name, value_data_type)
      data_type = Arrow::ListDataType.new(value_field)
      builder = Arrow::ListArrayBuilder.new(data_type)
      values_list.each do |values|
        if values.nil?
          builder.append_null
        else
          append_to_builder(builder, values)
        end
      end
      builder.finish
    end

    def build_struct_array(fields, structs)
      data_type = Arrow::StructDataType.new(fields)
      builder = Arrow::StructArrayBuilder.new(data_type)
      structs.each do |struct|
        if struct.nil?
          builder.append_null
        else
          append_to_builder(builder, struct)
        end
      end
      builder.finish
    end

    def append_to_builder(builder, value)
      if value.nil?
        builder.append_null
      else
        data_type = builder.value_data_type
        case data_type
        when Arrow::ListDataType
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

    def build_table(arrays)
      fields = arrays.collect do |name, array|
        Arrow::Field.new(name, array.value_data_type)
      end
      schema = Arrow::Schema.new(fields)
      columns = arrays.collect.with_index do |(_name, array), i|
        Arrow::Column.new(fields[i], array)
      end
      Arrow::Table.new(schema, columns)
    end

    def build_record_batch(arrays)
      n_rows = arrays.collect {|_, array| array.length}.min || 0
      fields = arrays.collect do |name, array|
        Arrow::Field.new(name, array.value_data_type)
      end
      schema = Arrow::Schema.new(fields)
      Arrow::RecordBatch.new(schema, n_rows, arrays.values)
    end

    private
    def build_array(builder, values)
      values.each do |value|
        if value.nil?
          builder.append_null
        else
          builder.append_value(value)
        end
      end
      builder.finish
    end
  end
end
