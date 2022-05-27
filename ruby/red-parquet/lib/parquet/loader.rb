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

module Parquet
  class Loader < GObjectIntrospection::Loader
    class << self
      def load
        super("Parquet", Parquet)
      end
    end

    private
    def post_load(repository, namespace)
      require_libraries
    end

    def require_libraries
      require "parquet/arrow-table-loadable"
      require "parquet/arrow-table-savable"
      require "parquet/writer-properties"
    end

    def load_object_info(info)
      super

      klass = @base_module.const_get(rubyish_class_name(info))
      if klass.method_defined?(:close)
        klass.extend(Arrow::BlockClosable)
      end
    end

    def load_method_info(info, klass, method_name)
      case klass.name
      when "Parquet::BooleanStatistics"
        case method_name
        when "min?"
          method_name = "min"
        when "max?"
          method_name = "max"
        end
        super(info, klass, method_name)
      else
        super
      end
    end
  end
end
