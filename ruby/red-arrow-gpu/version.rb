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

version_rb_path = Pathname.new(__FILE__)
base_dir = version_rb_path.dirname
pom_xml_path = base_dir.join("..", "..", "java", "pom.xml")
lib_version_rb_path = base_dir.join("lib", "arrow-gpu", "version.rb")

need_update = false
if not lib_version_rb_path.exist?
  need_update = true
elsif version_rb_path.mtime > lib_version_rb_path.mtime
  need_update = true
elsif pom_xml_path.exist? and pom_xml_path.mtime > lib_version_rb_path.mtime
  need_update = true
end

if need_update
  version = pom_xml_path.read.scan(/^  <version>(.+?)<\/version>/)[0][0]
  major, minor, micro, tag = version.split(/[.-]/)
  lib_version_rb_path.open("w") do |lib_version_rb|
    lib_version_rb.puts(<<-RUBY)
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

module ArrowGPU
  module Version
    MAJOR = #{major}
    MINOR = #{minor}
    MICRO = #{micro}
    TAG = #{tag ? tag.dump : nil}
    STRING = #{version.dump}
  end

  VERSION = Version::STRING
end
    RUBY
  end
end

require_relative "lib/arrow-gpu/version"
