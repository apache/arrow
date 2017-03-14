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
typelib_dir = base_dir + "arrow-glib"
test_dir = base_dir + "test"

ENV["GI_TYPELIB_PATH"] = [
  typelib_dir.to_s,
  ENV["GI_TYPELIB_PATH"],
].compact.join(File::PATH_SEPARATOR)

require "gi"

Arrow = GI.load("Arrow")
ArrowIO = GI.load("ArrowIO")
ArrowIPC = GI.load("ArrowIPC")

require "tempfile"
require_relative "helper/buildable"

exit(Test::Unit::AutoRunner.run(true, test_dir.to_s))
