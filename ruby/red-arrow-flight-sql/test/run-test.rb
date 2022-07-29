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

$VERBOSE = true

require "pathname"

(ENV["ARROW_DLL_PATH"] || "").split(File::PATH_SEPARATOR).each do |path|
  RubyInstaller::Runtime.add_dll_directory(path)
end

base_dir = Pathname.new(__dir__).parent.expand_path
arrow_base_dir = base_dir.parent + "red-arrow"
arrow_flight_lib_dir = base_dir.parent + "red-arrow-flight" + "lib"

lib_dir = base_dir + "lib"
test_dir = base_dir + "test"

arrow_lib_dir = arrow_base_dir + "lib"
arrow_ext_dir = arrow_base_dir + "ext" + "arrow"

build_dir = ENV["BUILD_DIR"]
if build_dir
  arrow_build_dir = Pathname.new(build_dir) + "red-arrow"
else
  arrow_build_dir = arrow_ext_dir
end

$LOAD_PATH.unshift(arrow_build_dir.to_s)
$LOAD_PATH.unshift(arrow_lib_dir.to_s)
$LOAD_PATH.unshift(arrow_flight_lib_dir.to_s)
$LOAD_PATH.unshift(lib_dir.to_s)

require_relative "helper"

exit(Test::Unit::AutoRunner.run(true, test_dir.to_s))
