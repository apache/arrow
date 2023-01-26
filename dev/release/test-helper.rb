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
require "fileutils"
require "find"
require 'net/http'
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
    git("tag").lines(chomp: true)
  end

  def parse_patch(patch)
    diffs = []
    in_hunk = false
    patch.each_line do |line|
      case line
      when /\A--- a\//
        path = $POSTMATCH.chomp
        diffs << { path: path, hunks: [] }
        in_hunk = false
      when /\A@@/
        in_hunk = true
        diffs.last[:hunks] << []
      when /\A[-+]/
        next unless in_hunk
        diffs.last[:hunks].last << line.chomp
      end
    end
    diffs.sort_by do |diff|
      diff[:path]
    end
  end
end

module VersionDetectable
  def release_type
    (data || {})[:release_type] || :major
  end

  def detect_versions
    top_dir = Pathname(__dir__).parent.parent
    cpp_cmake_lists = top_dir + "cpp" + "CMakeLists.txt"
    @snapshot_version = cpp_cmake_lists.read[/ARROW_VERSION "(.+?)"/, 1]
    @snapshot_major_version = @snapshot_version.split(".")[0]
    @release_version = @snapshot_version.gsub(/-SNAPSHOT\z/, "")
    @release_compatible_version = @release_version.split(".")[0, 2].join(".")
    @so_version = compute_so_version(@release_version)
    next_version_components = @release_version.split(".")
    case release_type
    when :major
      next_version_components[0].succ!
    when :minor
      next_version_components[1].succ!
    when :patch
      next_version_components[2].succ!
    else
      raise "unknown release type: #{release_type.inspect}"
    end
    @next_version = next_version_components.join(".")
    @next_major_version = @next_version.split(".")[0]
    @next_compatible_version = @next_version.split(".")[0, 2].join(".")
    @next_snapshot_version = "#{@next_version}-SNAPSHOT"
    @next_so_version = compute_so_version(@next_version)
    r_description = top_dir + "r" + "DESCRIPTION"
    @previous_version = r_description.read[/^Version: (.+?)\.9000$/, 1]
    if @previous_version
      @previous_compatible_version = @previous_version.split(".")[0, 2].join(".")
    else
      @previous_compatible_version = nil
    end
  end

  def compute_so_version(version)
    major, minor, _patch = version.split(".")
    Integer(major, 10) * 100 + Integer(minor, 10)
  end

  def on_release_branch?
    @snapshot_version == @release_version
  end

  def omit_on_release_branch
    omit("Not for release branch") if on_release_branch?
  end
end
