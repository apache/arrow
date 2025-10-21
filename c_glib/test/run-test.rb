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

(ENV["ARROW_DLL_PATH"] || "").split(File::PATH_SEPARATOR).each do |path|
  RubyInstaller::Runtime.add_dll_directory(path)
end

base_dir = Pathname(__dir__).parent
test_dir = base_dir + "test"

require "gi"

Gio = GI.load("Gio")
Arrow = GI.load("Arrow")
Arrow.compute_initialize
module Arrow
  class Buffer
    alias_method :initialize_raw, :initialize
    def initialize(data)
      initialize_raw(data)
      @data = data
    end
  end

  class BooleanScalar
    alias_method :value, :value?
  end
end

begin
  ArrowCUDA = GI.load("ArrowCUDA")
rescue GObjectIntrospection::RepositoryError::TypelibNotFound
end

begin
  ArrowDataset = GI.load("ArrowDataset")
rescue GObjectIntrospection::RepositoryError::TypelibNotFound
end

begin
  class ArrowFlightLoader < GI::Loader
    def should_unlock_gvl?(info, klass)
      true
    end
  end
  flight_module = Module.new
  ArrowFlightLoader.load("ArrowFlight", flight_module)
  ArrowFlight = flight_module
  GObjectIntrospection::Loader.start_callback_dispatch_thread
rescue GObjectIntrospection::RepositoryError::TypelibNotFound
end

begin
  class ArrowFlightSQLLoader < GI::Loader
    def should_unlock_gvl?(info, klass)
      true
    end
  end
  flight_sql_module = Module.new
  ArrowFlightSQLLoader.load("ArrowFlightSQL", flight_sql_module)
  ArrowFlightSQL = flight_sql_module
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

require "fileutils"
require "find"
require "rbconfig"
require "stringio"
require "tempfile"
require "zlib"
require_relative "helper/buildable"
require_relative "helper/data-type"
require_relative "helper/fixture"
if defined?(ArrowFlight)
  require_relative "helper/flight-server"
end
if defined?(ArrowFlightSQL)
  require_relative "helper/flight-sql-server"
end
require_relative "helper/omittable"
require_relative "helper/readable"
require_relative "helper/writable"

exit(Test::Unit::AutoRunner.run(true, test_dir.to_s))
