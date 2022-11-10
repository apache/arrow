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
  class RawTensorConverter
    attr_reader :data_type
    attr_reader :data
    attr_reader :shape
    attr_reader :strides
    attr_reader :dimension_names
    def initialize(raw_tensor,
                   data_type: nil,
                   shape: nil,
                   strides: nil,
                   dimension_names: nil)
      @raw_tensor = raw_tensor
      @data_type = data_type
      @data = nil
      @shape = shape
      @strides = strides
      @dimension_names = dimension_names
      convert
    end

    private
    def convert
      case @raw_tensor
      when Buffer
        @data = @raw_tensor
      when String
        unless @raw_tensor.encoding == Encoding::ASCII_8BIT
          message = "raw tensor String must be an ASCII-8BIT encoded string: " +
                    "#{@raw_tensor.encoding.inspect}"
          raise ArgumentError, message
        end
        @data = Arrow::Buffer.new(@raw_tensor)
      else
        @shape ||= guess_shape
        build_buffer
        unless @strides.nil?
          message = "strides: is only accepted with " +
                    "an Arrow::Buffer or String raw tensor: #{@strides.inspect}"
          raise ArgumentError, message
        end
      end
      if @shape.nil?
        raise ArgumentError, "shape: is missing: #{@raw_tensor.inspect}"
      end
      if @data_type.nil?
        raise ArgumentError, "data_type: is missing: #{@raw_tensor.inspect}"
      end
    end

    def guess_shape
      shape = [@raw_tensor.size]
      target = @raw_tensor[0]
      while target.is_a?(::Array)
        shape << target.size
        target = target[0]
      end
      shape
    end

    def build_buffer
      if @data_type
        @data_type = DataType.resolve(@data_type)
        array = @data_type.build_array(@raw_tensor.flatten)
      else
        array = Array.new(@raw_tensor.flatten)
        @data_type = array.value_data_type
      end
      @data = array.data_buffer
    end
  end
end
