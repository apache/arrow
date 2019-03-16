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

ENV["TZ"] = "Asia/Tokyo"

$VERBOSE = true

require "pathname"

base_dir = Pathname.new(__dir__).parent.expand_path

lib_dir = base_dir + "lib"
ext_dir = base_dir + "ext" + "arrow"
test_dir = base_dir + "test"

make = nil
if ENV["NO_MAKE"] != "yes"
  if ENV["MAKE"]
    make = ENV["MAKE"]
  elsif system("type gmake > /dev/null")
    make = "gmake"
  elsif system("type make > /dev/null")
    make = "make"
  end
end
if make
  Dir.chdir(ext_dir.to_s) do
    unless File.exist?("Makefile")
      system(RbConfig.ruby, "extconf.rb", "--enable-debug-build") or exit(false)
    end
    system("#{make} > /dev/null") or exit(false)
  end
end

$LOAD_PATH.unshift(ext_dir.to_s)
$LOAD_PATH.unshift(lib_dir.to_s)

require_relative "helper"

ENV["TEST_UNIT_MAX_DIFF_TARGET_STRING_SIZE"] ||= "10000"

exit(Test::Unit::AutoRunner.run(true, test_dir.to_s))
