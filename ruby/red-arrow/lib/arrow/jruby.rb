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

if File.exist?("../red-arrow_jars")
  # installed gems
  require_relative "../red-arrow_jars"
else
  # local development
  require "red-arrow_jars"
end

module Arrow
  class << self
    def allocator
      @allocator ||= org.apache.arrow.memory.RootAllocator.new
    end
  end
end

require_relative "jruby/array"
require_relative "jruby/array-builder"
require_relative "jruby/chunked-array"
require_relative "jruby/compression-type"
require_relative "jruby/csv-read-options"
require_relative "jruby/decimal128"
require_relative "jruby/decimal256"
require_relative "jruby/error"
require_relative "jruby/file-system"
require_relative "jruby/function"
require_relative "jruby/record-batch"
require_relative "jruby/record-batch-iterator"
require_relative "jruby/sort-key"
require_relative "jruby/sort-options"
require_relative "jruby/stream-listener-raw"
require_relative "jruby/table"
require_relative "jruby/writable"

require_relative "libraries"
