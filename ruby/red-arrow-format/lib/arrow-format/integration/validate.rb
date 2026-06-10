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

require "json"
require "test-unit"

ENV["TEST_UNIT_MAX_DIFF_TARGET_STRING_SIZE"] ||= "1_000_000"

class ArrowFormatIntegrationTest < Test::Unit::TestCase
  module ToHashable
    refine ArrowFormat::Type do
      def to_h
        {
          "name" => normalized_name,
        }
      end

      private
      def normalized_name
        name.downcase
      end
    end

    refine ArrowFormat::BooleanType do
      private
      def normalized_name
        "bool"
      end
    end

    refine ArrowFormat::IntType do
      def to_h
        super.merge("bitWidth" => @bit_width,
                    "isSigned" => @signed)
      end

      private
      def normalized_name
        "int"
      end
    end

    refine ArrowFormat::FloatingPointType do
      def to_h
        super.merge("precision" => normalized_precision)
      end

      private
      def normalized_name
        "floatingpoint"
      end

      def normalized_precision
        @precision.to_s.upcase
      end
    end

    refine ArrowFormat::DateType do
      def to_h
        super.merge("unit" => normalized_unit)
      end

      private
      def normalized_name
        "date"
      end

      def normalized_unit
        @unit.to_s.upcase
      end
    end

    refine ArrowFormat::TimeType do
      def to_h
        super.merge("bitWidth" => @bit_width,
                    "unit" => normalized_unit)
      end

      private
      def normalized_name
        "time"
      end

      def normalized_unit
        @unit.to_s.upcase
      end
    end

    refine ArrowFormat::TimestampType do
      def to_h
        hash = super
        hash["unit"] = normalized_unit
        hash["timezone"] = @time_zone if @time_zone
        hash
      end

      private
      def normalized_name
        "timestamp"
      end

      def normalized_unit
        @unit.to_s.upcase
      end
    end

    refine ArrowFormat::IntervalType do
      def to_h
        super.merge("unit" => normalized_unit)
      end

      private
      def normalized_name
        "interval"
      end

      def normalized_unit
        @unit.to_s.upcase
      end
    end

    refine ArrowFormat::DurationType do
      def to_h
        super.merge("unit" => normalized_unit)
      end

      private
      def normalized_unit
        @unit.to_s.upcase
      end
    end

    refine ArrowFormat::FixedSizeBinaryType do
      def to_h
        super.merge("byteWidth" => @byte_width)
      end
    end

    refine ArrowFormat::FixedSizeListType do
      def to_h
        super.merge("listSize" => @size)
      end
    end

    refine ArrowFormat::DecimalType do
      def to_h
        hash = super
        hash.delete("byteWidth")
        hash["bitWidth"] = @byte_width * 8
        hash["precision"] = @precision
        hash["scale"] = @scale
        hash
      end

      private
      def normalized_name
        "decimal"
      end
    end

    refine ArrowFormat::UnionType do
      def to_h
        super.merge("mode" => normalized_mode,
                    "typeIds" => normalized_type_ids)
      end

      private
      def normalized_name
        "union"
      end

      def normalized_mode
        @mode.to_s.upcase
      end

      def normalized_type_ids
        @type_ids
      end
    end

    refine ArrowFormat::MapType do
      def to_h
        super.merge("keysSorted" => @keys_sorted)
      end
    end

    module MetadataNormalizable
      private
      def normalized_metadata
        metadata_list = @metadata.collect do |key, value|
          {"key" => key, "value" => value}
        end
        metadata_list.sort_by do |metadatum|
          metadatum["key"]
        end
      end
    end

    refine ArrowFormat::Field do
      import_methods MetadataNormalizable

      def to_h
        hash = {
          "children" => normalized_children,
          "name" => @name,
          "nullable" => @nullable,
        }
        if @type.is_a?(ArrowFormat::DictionaryType)
          hash["type"] = @type.value_type.to_h
          hash["dictionary"] = {
            "id" => @type.id,
            "indexType" => @type.index_type.to_h,
            "isOrdered" => @type.ordered?,
          }
        else
          hash["type"] = @type.to_h
        end
        hash["metadata"] = normalized_metadata if @metadata
        hash
      end

      private
      def normalized_children
        if @type.respond_to?(:children)
          @type.children.collect(&:to_h)
        elsif @type.respond_to?(:child)
          [@type.child.to_h]
        else
          []
        end
      end
    end

    refine ArrowFormat::Schema do
      import_methods MetadataNormalizable

      def to_h
        hash = {
          "fields" => @fields.collect(&:to_h),
        }
        hash["metadata"] = normalized_metadata if @metadata
        hash
      end
    end

    refine ArrowFormat::Array do
      def to_h
        hash = {
          "count" => size,
        }
        to_h_data(hash)
        hash
      end

      private
      def to_h_data(hash)
        hash["DATA"] = normalized_data
        hash["VALIDITY"] = normalized_validity
      end

      def normalized_data
        to_a
      end

      def normalized_validity
        if @validity_buffer
          validity_bitmap.collect {|valid| valid ? 1 : 0}
        else
          [1] * @size
        end
      end

      def stringify_data(data)
        data.collect do |value|
          if value.nil?
            nil
          else
            value.to_s
          end
        end
      end

      def hexify(value)
        value.each_byte.collect {|byte| "%02X" % byte}.join("")
      end

      def normalized_children
        @children.zip(@type.children).collect do |child, field|
          normalize_child(child, field)
        end
      end

      def normalize_child(child, field)
        child.to_h.merge("name" => field.name)
      end
    end

    refine ArrowFormat::NullArray do
      private
      def to_h_data(hash)
      end
    end

    refine ArrowFormat::Int64Array do
      private
      def normalized_data
        stringify_data(super)
      end
    end

    refine ArrowFormat::UInt64Array do
      private
      def normalized_data
        stringify_data(super)
      end
    end

    refine ArrowFormat::Float32Array do
      private
      def normalized_data
        super.collect do |value|
          if value.nil?
            nil
          else
            value.round(3)
          end
        end
      end
    end

    refine ArrowFormat::Date64Array do
      private
      def normalized_data
        stringify_data(super)
      end
    end

    refine ArrowFormat::Time64Array do
      private
      def normalized_data
        stringify_data(super)
      end
    end

    refine ArrowFormat::TimestampArray do
      private
      def normalized_data
        stringify_data(super)
      end
    end

    refine ArrowFormat::DayTimeIntervalArray do
      private
      def normalized_data
        super.collect do |value|
          if value.nil?
            nil
          else
            day, time = value
            {"days" => day, "milliseconds" => time}
          end
        end
      end
    end

    refine ArrowFormat::MonthDayNanoIntervalArray do
      private
      def normalized_data
        super.collect do |value|
          if value.nil?
            nil
          else
            month, day, nano = value
            {"months" => month, "days" => day, "nanoseconds" => nano}
          end
        end
      end
    end

    refine ArrowFormat::MonthDayNanoIntervalArray do
      private
      def normalized_data
        super.collect do |value|
          if value.nil?
            nil
          else
            month, day, nano = value
            {"months" => month, "days" => day, "nanoseconds" => nano}
          end
        end
      end
    end

    refine ArrowFormat::DurationArray do
      private
      def normalized_data
        stringify_data(super)
      end
    end

    refine ArrowFormat::VariableSizeBinaryArray do
      private
      def to_h_data(hash)
        super(hash)
        hash["OFFSET"] = normalized_offsets
      end

      def normalized_data
        super.collect do |value|
          if value.nil?
            nil
          else
            normalize_value(value)
          end
        end
      end

      def normalize_value(value)
        hexify(value)
      end

      def normalized_offsets
        offsets
      end
    end

    refine ArrowFormat::LargeBinaryArray do
      private
      def normalized_offsets
        offsets.collect(&:to_s)
      end
    end

    refine ArrowFormat::VariableSizeUTF8Array do
      private
      def normalize_value(value)
        value
      end
    end

    refine ArrowFormat::LargeUTF8Array do
      private
      def normalized_offsets
        offsets.collect(&:to_s)
      end
    end

    refine ArrowFormat::FixedSizeBinaryArray do
      private
      def normalized_data
        super.collect do |value|
          if value.nil?
            nil
          else
            normalize_value(value)
          end
        end
      end

      def normalize_value(value)
        hexify(value)
      end
    end

    refine ArrowFormat::DecimalArray do
      private
      def normalize_value(value)
        (value * (10 ** (@type.scale))).to_s("f").delete_suffix(".0")
      end
    end

    refine ArrowFormat::VariableSizeListArray do
      private
      def to_h_data(hash)
        hash["OFFSET"] = normalized_offsets
        hash["VALIDITY"] = normalized_validity
        hash["children"] = [normalize_child(@child, @type.child)]
      end

      def normalized_offsets
        offsets
      end
    end

    refine ArrowFormat::FixedSizeListArray do
      private
      def to_h_data(hash)
        hash["VALIDITY"] = normalized_validity
        hash["children"] = [normalize_child(@child, @type.child)]
      end
    end

    refine ArrowFormat::LargeListArray do
      private
      def normalized_offsets
        offsets.collect(&:to_s)
      end
    end

    refine ArrowFormat::StructArray do
      private
      def to_h_data(hash)
        hash["VALIDITY"] = normalized_validity
        hash["children"] = normalized_children
      end
    end

    refine ArrowFormat::UnionArray do
      private
      def to_h_data(hash)
        hash["TYPE_ID"] = each_type.to_a
        hash["children"] = normalized_children
      end
    end

    refine ArrowFormat::DenseUnionArray do
      private
      def to_h_data(hash)
        super
        hash["OFFSET"] = each_offset.to_a
      end
    end

    refine ArrowFormat::DictionaryArray do
      private
      def normalized_data
        indices
      end
    end

    refine ArrowFormat::RecordBatch do
      def to_h
        {
          "columns" => normalized_columns,
          "count" => @n_rows,
        }
      end

      private
      def normalized_columns
        @schema.fields.zip(@columns).collect do |field, column|
          column.to_h.merge("name" => field.name)
        end
      end
    end
  end

  using ToHashable

  def setup
    @options = ArrowFormat::Integration::Options.singleton
  end

  def normalize_field!(field)
    metadata = field["metadata"]
    if metadata
      field["metadata"] = metadata.sort_by do |metadatum|
        metadatum["key"]
      end
    end

    case field["type"]["name"]
    when "decimal"
      field["type"]["bitWidth"] ||= 128 unless @options.validate_decimal?
    when "map"
      entries = field["children"][0]
      entries["name"] = "entries"
      entries["children"][0]["name"] = "key"
      entries["children"][1]["name"] = "value"
    end
  end

  def normalize_schema!(schema)
    schema["fields"].each do |field|
      normalize_field!(field)
    end
  end

  def normalize_array!(array, field)
    case field["type"]["name"]
    when "map"
      entries = array["children"][0]
      entries["name"] = "entries"
      entries["children"][0]["name"] = "key"
      entries["children"][1]["name"] = "value"
    when "union"
      # V4 data has VALIDITY.
      array.delete("VALIDITY")
    end

    data = array["DATA"]
    validity = array["VALIDITY"]
    if data and validity
      array["DATA"] = data.zip(validity).collect do |value, valid_bit|
        if (valid_bit == 1)
          value
        else
          nil
        end
      end
    end

    child_arrays = array["children"]
    if child_arrays
      child_fields = field["children"]
      child_arrays.zip(child_fields) do |child_array, child_field|
        normalize_array!(child_array, child_field)
      end
    end
  end

  def normalize_record_batch!(record_batch, schema)
    record_batch["columns"].zip(schema["fields"]) do |column, field|
      normalize_array!(column, field)
    end
  end

  def test_validate
    expected = JSON.parse(File.read(@options.json))
    expected_schema = expected["schema"]
    normalize_schema!(expected_schema)
    File.open(@options.arrow, "rb") do |input|
      reader = ArrowFormat::FileReader.new(input)
      expected_record_batches = []
      actual_record_batches = []
      reader.each.with_index do |record_batch, i|
        expected_record_batch = expected["batches"][i]
        normalize_record_batch!(expected_record_batch, expected_schema)
        expected_record_batches << expected_record_batch
        actual_record_batches << record_batch.to_h
      end
      assert_equal({
                     schema: expected_schema,
                     record_batches: expected_record_batches,
                   },
                   {
                     schema: reader.schema.to_h,
                     record_batches: actual_record_batches,
                   })
    end
  end
end
