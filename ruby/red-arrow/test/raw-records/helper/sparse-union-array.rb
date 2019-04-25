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

module RawRecordsSparseUnionArrayHelper
  def fields(type, type_codes)
    field_description = {}
    if type.is_a?(Hash)
      field_description = field_description.merge(type)
    else
      field_description[:type] = type
    end
    {
      column: {
        type: :sparse_union,
        fields: [
          field_description.merge(name: "0"),
          field_description.merge(name: "1"),
        ],
        type_codes: type_codes,
      },
    }
  end

  # TODO: Use Arrow::RecordBatch.new(fields(type), records)
  def build_record_batch(type, records)
    type_codes = [0, 1]
    schema = Arrow::Schema.new(fields(type, type_codes))
    type_ids = []
    arrays = schema.fields[0].data_type.fields.collect do |field|
      sub_schema = Arrow::Schema.new([field])
      sub_records = records.collect do |record|
        [record[0].nil? ? nil : record[0][field.name]]
      end
      sub_record_batch = Arrow::RecordBatch.new(sub_schema,
                                                sub_records)
      sub_record_batch.columns[0]
    end
    records.each do |record|
      column = record[0]
      if column.nil?
        type_ids << nil
      elsif column.key?("0")
        type_ids << type_codes[0]
      elsif column.key?("1")
        type_ids << type_codes[1]
      end
    end
    union_array = Arrow::SparseUnionArray.new(schema.fields[0].data_type,
                                              Arrow::Int8Array.new(type_ids),
                                              arrays)
    schema = Arrow::Schema.new(column: union_array.value_data_type)
    Arrow::RecordBatch.new(schema,
                           records.size,
                           [union_array])
  end
end
