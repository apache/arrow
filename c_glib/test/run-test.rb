#!/usr/bin/env ruby
#
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

require "pathname"
require "test-unit"

base_dir = Pathname(__dir__).parent
test_dir = base_dir + "test"

require "gi"

Gio = GI.load("Gio")
Arrow = GI.load("Arrow")
module Arrow
  class Buffer
    alias_method :initialize_raw, :initialize
    def initialize(data)
      initialize_raw(data)
      @data = data
    end
  end
end

begin
  ArrowGPU = GI.load("ArrowGPU")
rescue GObjectIntrospection::RepositoryError::TypelibNotFound
end

begin
  Gandiva = GI.load("Gandiva")
rescue GObjectIntrospection::RepositoryError::TypelibNotFound
end

begin
  Parquet = GI.load("Parquet")
rescue GObjectIntrospection::RepositoryError::TypelibNotFound
end

begin
  Plasma = GI.load("Plasma")
rescue GObjectIntrospection::RepositoryError::TypelibNotFound
end

require "fileutils"
require "rbconfig"
require "tempfile"
require_relative "helper/buildable"
require_relative "helper/fixture"
require_relative "helper/omittable"
require_relative "helper/plasma-store"

exit(Test::Unit::AutoRunner.run(true, test_dir.to_s))
