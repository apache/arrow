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
    def build_boolean_array(values)
      build_array(Arrow::BooleanArrayBuilder, values)
    end

    def build_int8_array(values)
      build_array(Arrow::Int8ArrayBuilder, values)
    end

    def build_uint8_array(values)
      build_array(Arrow::UInt8ArrayBuilder, values)
    end

    def build_int16_array(values)
      build_array(Arrow::Int16ArrayBuilder, values)
    end

    def build_uint16_array(values)
      build_array(Arrow::UInt16ArrayBuilder, values)
    end

    def build_int32_array(values)
      build_array(Arrow::Int32ArrayBuilder, values)
    end

    def build_uint32_array(values)
      build_array(Arrow::UInt32ArrayBuilder, values)
    end

    def build_int64_array(values)
      build_array(Arrow::Int64ArrayBuilder, values)
    end

    def build_uint64_array(values)
      build_array(Arrow::UInt64ArrayBuilder, values)
    end

    def build_float_array(values)
      build_array(Arrow::FloatArrayBuilder, values)
    end

    def build_double_array(values)
      build_array(Arrow::DoubleArrayBuilder, values)
    end

    private
    def build_array(builder_class, values)
      builder = builder_class.new
      values.each do |value|
        if value.nil?
          builder.append_null
        else
          builder.append(value)
        end
      end
      builder.finish
    end
  end
end
