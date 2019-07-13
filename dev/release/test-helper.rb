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

require "English"
require "cgi/util"
require "find"
require "json"
require "open-uri"
require "rexml/document"
require "tempfile"
require "tmpdir"

module CommandRunnable
  class Error < StandardError
  end

  def sh(*command_line, check_result: true)
    if command_line[0].is_a?(Hash)
      env = command_line.shift
    else
      env = {}
    end
    stdout = Tempfile.new("command-stdout.log")
    stderr = Tempfile.new("command-stderr.log")
    success = system(env, *command_line, out: stdout.path, err: stderr.path)
    if check_result
      unless success
        message = "Failed to run: #{command_line.join(" ")}\n"
        message << "stdout:\n #{stdout.read}\n"
        message << "stderr:\n #{stderr.read}"
        raise Error, message
      end
    end
    stdout.read
  end
end

module GitRunnable
  include CommandRunnable

  def git(*args)
    if args[0].is_a?(Hash)
      env = args.shift
    else
      env = {}
    end
    sh(env, "git", *args)
  end

  def git_current_commit
    git("rev-parse", "HEAD").chomp
  end

  def git_tags
    git("tags").lines(chomp: true)
  end
end

module VersionDetectable
  def detect_versions
    top_dir = Pathname(__dir__).parent.parent
    cpp_cmake_lists = top_dir + "cpp" + "CMakeLists.txt"
    @snapshot_version = cpp_cmake_lists.read[/ARROW_VERSION "(.+?)"/, 1]
    @release_version = @snapshot_version.gsub(/-SNAPSHOT\z/, "")
    @next_version = @release_version.gsub(/\A\d+/) {|major| major.succ}
    r_description = top_dir + "r" + "DESCRIPTION"
    @previous_version = r_description.read[/^Version: (.+?)\.9000$/, 1]
  end
end
