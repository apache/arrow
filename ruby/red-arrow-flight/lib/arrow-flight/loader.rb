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

module ArrowFlight
  class Loader < GObjectIntrospection::Loader
    class << self
      def load
        super("ArrowFlight", ArrowFlight)
      end
    end

    private
    def post_load(repository, namespace)
      require_libraries
    end

    def require_libraries
      require "arrow-flight/call-options"
      require "arrow-flight/client"
      require "arrow-flight/client-options"
      require "arrow-flight/location"
      require "arrow-flight/record-batch-reader"
      require "arrow-flight/server-options"
      require "arrow-flight/ticket"
    end

    def prepare_function_info_lock_gvl(function_info, klass)
      super
      function_info.lock_gvl_default = false
    end
  end
end
