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

require "arrow/block-closable"

module Arrow
  class Loader < GObjectIntrospection::Loader
    class << self
      def load
        super("Arrow", Arrow)
      end
    end

    private
    def post_load(repository, namespace)
      require_libraries
    end

    def require_libraries
      require "arrow/array"
      require "arrow/array-builder"
      require "arrow/chunked-array"
      require "arrow/column"
      require "arrow/compression-type"
      require "arrow/csv-loader"
      require "arrow/csv-read-options"
      require "arrow/data-type"
      require "arrow/date32-array"
      require "arrow/date32-array-builder"
      require "arrow/date64-array"
      require "arrow/date64-array-builder"
      require "arrow/field"
      require "arrow/file-output-stream"
      require "arrow/list-data-type"
      require "arrow/path-extension"
      require "arrow/record"
      require "arrow/record-batch"
      require "arrow/record-batch-file-reader"
      require "arrow/record-batch-stream-reader"
      require "arrow/rolling-window"
      require "arrow/schema"
      require "arrow/slicer"
      require "arrow/struct-array"
      require "arrow/struct-data-type"
      require "arrow/table"
      require "arrow/table-formatter"
      require "arrow/table-list-formatter"
      require "arrow/table-table-formatter"
      require "arrow/table-loader"
      require "arrow/table-saver"
      require "arrow/tensor"
      require "arrow/timestamp-array"
      require "arrow/timestamp-array-builder"
      require "arrow/writable"
    end

    def load_object_info(info)
      super

      klass = @base_module.const_get(rubyish_class_name(info))
      if klass.method_defined?(:close)
        klass.extend(BlockClosable)
      end
    end

    def load_method_info(info, klass, method_name)
      case klass.name
      when "Arrow::StringArray"
        case method_name
        when "get_value"
          method_name = "get_raw_value"
        when "get_string"
          method_name = "get_value"
        end
        super(info, klass, method_name)
      when "Arrow::TimestampArray", "Arrow::Date32Array", "Arrow::Date64Array"
        case method_name
        when "get_value"
          method_name = "get_raw_value"
        end
        super(info, klass, method_name)
      else
       super
      end
    end
  end
end
