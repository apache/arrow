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

require "minitest/autorun"
require "rake"

package_dir = File.expand_path("..", __dir__)
Dir.chdir(package_dir) do
  load "Rakefile"
end

class DebianRuntimeDependenciesTest < Minitest::Test
  def setup
    @task = ApacheArrowPackageTask.new
    @control_template = File.read(File.join(__dir__, "../debian/control.in"))
    @libarrow_package = @control_template[/^Package: (libarrow\d+)$/, 1]
    refute_nil(@libarrow_package, "missing libarrow package stanza")
  end

  def rendered_control(target)
    @task.send(:apt_prepare_debian_control, @control_template, target)
  end

  def runtime_dependencies(control, package)
    stanza = control.split(/^Package: /).find do |candidate|
      candidate.start_with?("#{package}\n")
    end
    refute_nil(stanza, "missing package stanza for #{package}")
    depends = stanza[/^Depends:\n((?:[ \t].*\n)+)/, 1]
    refute_nil(depends, "missing Depends field for #{package}")
    depends.lines.reject do |line|
      line.lstrip.start_with?("#")
    end.join
  end

  def test_system_open_telemetry_target_requires_its_runtime_package
    %w[debian-trixie ubuntu-resolute].each do |target|
      dependencies = runtime_dependencies(rendered_control(target),
                                          @libarrow_package)
      assert_match(/\bopentelemetry-cpp\b/, dependencies, target)
    end
  end

  def test_target_without_system_open_telemetry_has_no_runtime_dependency
    %w[ubuntu-jammy ubuntu-noble].each do |target|
      dependencies = runtime_dependencies(rendered_control(target),
                                          @libarrow_package)
      refute_match(/\bopentelemetry-cpp\b/, dependencies, target)
    end
  end
end
