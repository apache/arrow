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

class PrepareTest < Test::Unit::TestCase
  include GitRunnable
  include VersionDetectable

  def setup
    @current_commit = git_current_commit
    detect_versions

    top_dir = Pathname(__dir__).parent.parent
    @original_git_repository = top_dir + ".git"
    Dir.mktmpdir do |dir|
      @test_git_repository = Pathname(dir) + "arrow"
      git("clone", @original_git_repository.to_s, @test_git_repository.to_s)
      Dir.chdir(@test_git_repository) do
        @tag_name = "apache-arrow-#{@release_version}"
        @release_branch = "release-#{@release_version}-rc0"
        @script = "dev/release/00-prepare.sh"
        git("checkout", "-b", @release_branch, @current_commit)
        yield
      end
    end
  end

  def prepare(*targets)
    env = {"PREPARE_DEFAULT" => "0"}
    targets.each do |target|
      env["PREPARE_#{target}"] = "1"
    end
    sh(env, @script, @release_version, @next_version)
  end

  def parse_patch(patch)
    diffs = []
    in_hunk = false
    patch.each_line do |line|
      case line
      when /\A--- a\//
        path = $POSTMATCH.chomp
        diffs << {path: path, hunks: []}
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

  def test_update_version_pre_tag
    prepare("VERSION_PRE_TAG")
    assert_equal([
                   {
                     path: "c_glib/configure.ac",
                     hunks: [
                       ["-m4_define([arrow_glib_version], #{@snapshot_version})",
                        "+m4_define([arrow_glib_version], #{@release_version})"],
                     ],
                   },
                   {
                     path: "c_glib/meson.build",
                     hunks: [
                       ["-version = '#{@snapshot_version}'",
                        "+version = '#{@release_version}'"],
                     ],
                   },
                   {
                     path: "ci/PKGBUILD",
                     hunks: [
                       ["-pkgver=#{@previous_version}.9000",
                        "+pkgver=#{@release_version}"],
                     ],
                   },
                   {
                     path: "cpp/CMakeLists.txt",
                     hunks: [
                       ["-set(ARROW_VERSION \"#{@snapshot_version}\")",
                        "+set(ARROW_VERSION \"#{@release_version}\")"],
                     ],
                   },
                   {
                     path: "csharp/Directory.Build.props",
                     hunks: [
                       ["-    <Version>#{@snapshot_version}</Version>",
                        "+    <Version>#{@release_version}</Version>"],
                     ],
                   },
                   {
                     path: "js/package.json",
                     hunks: [
                       ["-  \"version\": \"#{@snapshot_version}\"",
                        "+  \"version\": \"#{@release_version}\""]
                     ],
                   },
                   {
                     path: "matlab/CMakeLists.txt",
                     hunks: [
                       ["-set(MLARROW_VERSION \"#{@snapshot_version}\")",
                        "+set(MLARROW_VERSION \"#{@release_version}\")"],
                     ],
                   },
                   {
                     path: "python/setup.py",
                     hunks: [
                       ["-default_version = '#{@snapshot_version}'",
                        "+default_version = '#{@release_version}'"],
                     ],
                   },
                   {
                     path: "r/DESCRIPTION",
                     hunks: [
                       ["-Version: #{@previous_version}.9000",
                        "+Version: #{@release_version}"],
                     ],
                   },
                   {
                     path: "r/NEWS.md",
                     hunks: [
                       ["-\# arrow #{@previous_version}.9000",
                        "+\# arrow #{@release_version}"],
                     ],
                   },
                   {
                     path: "ruby/red-arrow-cuda/lib/arrow-cuda/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@snapshot_version}\"",
                        "+  VERSION = \"#{@release_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-arrow/lib/arrow/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@snapshot_version}\"",
                        "+  VERSION = \"#{@release_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-gandiva/lib/gandiva/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@snapshot_version}\"",
                        "+  VERSION = \"#{@release_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-parquet/lib/parquet/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@snapshot_version}\"",
                        "+  VERSION = \"#{@release_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-plasma/lib/plasma/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@snapshot_version}\"",
                        "+  VERSION = \"#{@release_version}\""],
                     ],
                   },
                   {
                     path: "rust/arrow/Cargo.toml",
                     hunks: [
                       ["-version = \"#{@snapshot_version}\"",
                        "+version = \"#{@release_version}\""],
                     ],
                   },
                   {
                     path: "rust/datafusion/Cargo.toml",
                     hunks: [
                       ["-version = \"#{@snapshot_version}\"",
                        "+version = \"#{@release_version}\""],
                       ["-arrow = { path = \"../arrow\" }",
                        "-parquet = { path = \"../parquet\" }",
                        "+arrow = \"#{@release_version}\"",
                        "+parquet = \"#{@release_version}\""]
                     ],
                   },
                   {
                     path: "rust/datafusion/README.md",
                     hunks: [
                       ["-datafusion = \"#{@snapshot_version}\"",
                        "+datafusion = \"#{@release_version}\""],
                     ],
                   },
                   {
                     path: "rust/parquet/Cargo.toml",
                     hunks: [
                       ["-version = \"#{@snapshot_version}\"",
                        "+version = \"#{@release_version}\""],
                       ["-arrow = { path = \"../arrow\" }",
                        "+arrow = \"#{@release_version}\""]
                     ],
                   },
                   {
                     path: "rust/parquet/README.md",
                     hunks: [
                       ["-parquet = \"#{@snapshot_version}\"",
                        "+parquet = \"#{@release_version}\""],
                       ["-See [crate documentation](https://docs.rs/crate/parquet/#{@snapshot_version}) on available API.",
                        "+See [crate documentation](https://docs.rs/crate/parquet/#{@release_version}) on available API."],
                     ],
                   },
                 ],
                 parse_patch(git("log", "-n", "1", "-p")))
  end

  def test_update_version_post_tag
    prepare("VERSION_PRE_TAG",
            "VERSION_POST_TAG")
    assert_equal([
                   {
                     path: "c_glib/configure.ac",
                     hunks: [
                       ["-m4_define([arrow_glib_version], #{@release_version})",
                        "+m4_define([arrow_glib_version], #{@next_version}-SNAPSHOT)"],
                     ],
                   },
                   {
                     path: "c_glib/meson.build",
                     hunks: [
                       ["-version = '#{@release_version}'",
                        "+version = '#{@next_version}-SNAPSHOT'"],
                     ],
                   },
                   {
                     path: "ci/PKGBUILD",
                     hunks: [
                       ["-pkgver=#{@release_version}",
                        "+pkgver=#{@release_version}.9000"],
                     ],
                   },
                   {
                     path: "cpp/CMakeLists.txt",
                     hunks: [
                       ["-set(ARROW_VERSION \"#{@release_version}\")",
                        "+set(ARROW_VERSION \"#{@next_version}-SNAPSHOT\")"],
                     ],
                   },
                   {
                     path: "csharp/Directory.Build.props",
                     hunks: [
                       ["-    <Version>#{@release_version}</Version>",
                        "+    <Version>#{@next_version}-SNAPSHOT</Version>"],
                     ],
                   },
                   {
                     path: "js/package.json",
                     hunks: [
                       ["-  \"version\": \"#{@release_version}\"",
                        "+  \"version\": \"#{@next_version}-SNAPSHOT\""],
                     ],
                   },
                   {
                     path: "matlab/CMakeLists.txt",
                     hunks: [
                       ["-set(MLARROW_VERSION \"#{@release_version}\")",
                        "+set(MLARROW_VERSION \"#{@next_version}-SNAPSHOT\")"],
                     ],
                   },
                   {
                     path: "python/setup.py",
                     hunks: [
                       ["-default_version = '#{@release_version}'",
                        "+default_version = '#{@next_version}-SNAPSHOT'"],
                     ],
                   },
                   {
                     path: "r/DESCRIPTION",
                     hunks: [
                       ["-Version: #{@release_version}",
                        "+Version: #{@release_version}.9000"],
                     ],
                   },
                   {
                     path: "r/NEWS.md",
                     # Note that these are additions only, no replacement
                     hunks: [
                       ["+# arrow #{@release_version}.9000",
                        "+"],
                     ],
                   },
                   {
                     path: "ruby/red-arrow-cuda/lib/arrow-cuda/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_version}-SNAPSHOT\""],
                     ],
                   },
                   {
                     path: "ruby/red-arrow/lib/arrow/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_version}-SNAPSHOT\""],
                     ],
                   },
                   {
                     path: "ruby/red-gandiva/lib/gandiva/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_version}-SNAPSHOT\""],
                     ],
                   },
                   {
                     path: "ruby/red-parquet/lib/parquet/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_version}-SNAPSHOT\""],
                     ],
                   },
                   {
                     path: "ruby/red-plasma/lib/plasma/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_version}-SNAPSHOT\""],
                     ],
                   },
                   {
                     path: "rust/arrow/Cargo.toml",
                     hunks: [
                       ["-version = \"#{@release_version}\"",
                        "+version = \"#{@next_version}-SNAPSHOT\""],
                     ],
                   },
                   {
                     path: "rust/datafusion/Cargo.toml",
                     hunks: [
                       ["-version = \"#{@release_version}\"",
                        "+version = \"#{@next_version}-SNAPSHOT\""],
                       ["-arrow = \"#{@release_version}\"",
                        "-parquet = \"#{@release_version}\"",
                        "+arrow = { path = \"../arrow\" }",
                        "+parquet = { path = \"../parquet\" }"]
                     ],
                   },
                   {
                     path: "rust/datafusion/README.md",
                     hunks: [
                       ["-datafusion = \"#{@release_version}\"",
                        "+datafusion = \"#{@next_version}-SNAPSHOT\""],
                     ],
                   },
                   {
                     path: "rust/parquet/Cargo.toml",
                     hunks: [
                       ["-version = \"#{@release_version}\"",
                        "+version = \"#{@next_version}-SNAPSHOT\""],
                       ["-arrow = \"#{@release_version}\"",
                        "+arrow = { path = \"../arrow\" }"]
                     ],
                   },
                   {
                     path: "rust/parquet/README.md",
                     hunks: [
                       ["-parquet = \"#{@release_version}\"",
                        "+parquet = \"#{@next_version}-SNAPSHOT\""],
                       ["-See [crate documentation](https://docs.rs/crate/parquet/#{@release_version}) on available API.",
                        "+See [crate documentation](https://docs.rs/crate/parquet/#{@next_version}-SNAPSHOT) on available API."],
                     ],
                   },
                 ],
                 parse_patch(git("log", "-n", "1", "-p")))
  end
end
