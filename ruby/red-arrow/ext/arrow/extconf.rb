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

require "extpp"
require "mkmf-gnome2"

unless required_pkg_config_package("arrow",
                                   debian: "libarrow-dev",
                                   redhat: "arrow-devel",
                                   homebrew: "apache-arrow",
                                   msys2: "arrow")
  exit(false)
end

unless required_pkg_config_package("arrow-glib",
                                   debian: "libarrow-glib-dev",
                                   redhat: "arrow-glib-devel",
                                   homebrew: "apache-arrow-glib",
                                   msys2: "arrow")
  exit(false)
end

[
  ["glib2", "ext/glib2"],
].each do |name, relative_source_dir|
  spec = find_gem_spec(name)
  source_dir = File.join(spec.full_gem_path, relative_source_dir)
  build_dir = source_dir
  add_depend_package_path(name, source_dir, build_dir)
end

create_makefile("arrow")
