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

require_relative "block-closable"

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
      require_extension_library
      gc_guard
      self.class.start_callback_dispatch_thread
      @base_module.compute_initialize
    end

    def require_libraries
      require_relative "libraries"
    end

    def require_extension_library
      require "arrow.so"
    end

    def gc_guard
      require_relative "constructor-arguments-gc-guardable"

      [
        @base_module::BinaryScalar,
        @base_module::Buffer,
        @base_module::DenseUnionScalar,
        @base_module::FixedSizeBinaryScalar,
        @base_module::LargeBinaryScalar,
        @base_module::LargeListScalar,
        @base_module::LargeStringScalar,
        @base_module::ListScalar,
        @base_module::MapScalar,
        @base_module::SparseUnionScalar,
        @base_module::StringScalar,
        @base_module::StructScalar,
      ].each do |klass|
        klass.prepend(ConstructorArgumentsGCGuardable)
      end
    end

    def rubyish_class_name(info)
      name = info.name
      case name
      when "StreamListener"
        "StreamListenerRaw"
      else
        super
      end
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
      when /Array\z/
        case method_name
        when "values"
          method_name = "values_raw"
        end
      end

      case klass.name
      when /Builder\z/
        case method_name
        when "append"
          return
        else
          super
        end
      when "Arrow::StringArray"
        case method_name
        when "get_value"
          method_name = "get_raw_value"
        when "get_string"
          method_name = "get_value"
        end
        super(info, klass, method_name)
      when "Arrow::Date32Array",
           "Arrow::Date64Array",
           "Arrow::Decimal128Array",
           "Arrow::Decimal256Array",
           "Arrow::HalfFloatArray",
           "Arrow::Time32Array",
           "Arrow::Time64Array",
           "Arrow::TimestampArray"
        case method_name
        when "get_value"
          method_name = "get_raw_value"
        end
        super(info, klass, method_name)
      when "Arrow::Decimal128", "Arrow::Decimal256"
        case method_name
        when "copy"
          method_name = "dup"
        end
        super(info, klass, method_name)
      when "Arrow::BooleanScalar"
        case method_name
        when "value?"
          method_name = "value"
        end
        super(info, klass, method_name)
      else
        super
      end
    end

    def prepare_function_info_lock_gvl(function_info, klass)
      super
      case klass.name
      when "Arrow::RecordBatchFileReader"
        case function_info.name
        when "new"
          function_info.lock_gvl_default = false
        end
      end
    end
  end
end
