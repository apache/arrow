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
        @release_branch = "testing-release-#{@release_version}-rc0"
        git("checkout", "-b", @release_branch, @current_commit)
        yield
      end
      FileUtils.rm_rf(@test_git_repository)
    end
  end

  def omit_on_release_branch
    omit("Not for release branch") if on_release_branch?
  end

  def prepare(*targets)
    if targets.last.is_a?(Hash)
      additional_env = targets.pop
    else
      additional_env = {}
    end
    env = { "PREPARE_DEFAULT" => "0" }
    targets.each do |target|
      env["PREPARE_#{target}"] = "1"
    end
    env = env.merge(additional_env)
    sh(env, "dev/release/01-prepare.sh", @release_version, @next_version, "0")
  end

  def bump_versions(*targets)
    env = { "BUMP_DEFAULT" => "0" }
    targets.each do |target|
      env["BUMP_#{target}"] = "1"
    end
    sh(env, "dev/release/post-12-bump-versions.sh", @release_version,
       @next_version)
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

  def test_linux_packages
    user = "Arrow Developers"
    email = "dev@arrow.apache.org"
    prepare("LINUX_PACKAGES", "DEBFULLNAME" => user, "DEBEMAIL" => email)
    changes = parse_patch(git("log", "-n", "1", "-p"))
    sampled_changes = changes.collect do |change|
      {
        path: change[:path],
        sampled_hunks: change[:hunks].collect(&:first),
      }
    end
    base_dir = "dev/tasks/linux-packages"
    today = Time.now.utc.strftime("%a %b %d %Y")
    expected_changes = [
      {
        path: "#{base_dir}/apache-arrow-apt-source/debian/changelog",
        sampled_hunks: [
          "+apache-arrow-apt-source (#{@release_version}-1) " +
          "unstable; urgency=low",
        ],
      },
      {
        path: "#{base_dir}/apache-arrow-release/yum/apache-arrow-release.spec.in",
        sampled_hunks: [
          "+* #{today} #{user} <#{email}> - #{@release_version}-1",
        ],
      },
      {
        path: "#{base_dir}/apache-arrow/debian/changelog",
        sampled_hunks: [
          "+apache-arrow (#{@release_version}-1) unstable; urgency=low",
        ],
      },
      {
        path: "#{base_dir}/apache-arrow/yum/arrow.spec.in",
        sampled_hunks: [
          "+* #{today} #{user} <#{email}> - #{@release_version}-1",
        ],
      },
    ]
    assert_equal(expected_changes, sampled_changes)
  end

  def test_version_pre_tag
    omit_on_release_branch
    prepare("VERSION_PRE_TAG")
    assert_equal([
                   {
                     path: "c_glib/meson.build",
                     hunks: [
                       ["-version = '#{@snapshot_version}'",
                        "+version = '#{@release_version}'"],
                     ],
                   },
                   {
                     path: "ci/scripts/PKGBUILD",
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
                     path: "cpp/vcpkg.json",
                     hunks: [
                       ["-  \"version-string\": \"#{@snapshot_version}\",",
                        "+  \"version-string\": \"#{@release_version}\","],
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
                     path: "dev/tasks/homebrew-formulae/apache-arrow.rb",
                     hunks: [
                       ["-  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@snapshot_version}/apache-arrow-#{@snapshot_version}.tar.gz\"",
                        "+  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@release_version}/apache-arrow-#{@release_version}.tar.gz\""],
                     ],
                   },
                   {
                     path: "dev/tasks/homebrew-formulae/autobrew/apache-arrow.rb",
                     hunks: [
                       ["-  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@previous_version}.9000/apache-arrow-#{@previous_version}.9000.tar.gz\"",
                        "+  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@release_version}/apache-arrow-#{@release_version}.tar.gz\""],
                     ],
                   },
                   {
                     path: "java/adapter/avro/pom.xml",
                     hunks: [
                       ["-    <version>#{@snapshot_version}</version>",
                        "+    <version>#{@release_version}</version>"],
                     ],
                   },
                   {
                     hunks: [
                       ["-        <version>#{@snapshot_version}</version>",
                        "+        <version>#{@release_version}</version>"],
                     ],
                     path: "java/adapter/jdbc/pom.xml",
                   },
                   {
                     hunks: [
                       ["-        <version>#{@snapshot_version}</version>",
                        "+        <version>#{@release_version}</version>"],
                     ],
                     path: "java/adapter/orc/pom.xml",
                   },
                   { hunks: [["-    <version>#{@snapshot_version}</version>",
                              "+    <version>#{@release_version}</version>"]],
                     path: "java/algorithm/pom.xml" },
                   { hunks: [["-    <version>#{@snapshot_version}</version>",
                              "+    <version>#{@release_version}</version>"]],
                     path: "java/compression/pom.xml" },
                   { hunks: [["-        <version>#{@snapshot_version}</version>",
                              "+        <version>#{@release_version}</version>"]],
                     path: "java/dataset/pom.xml" },
                   { hunks: [["-    <version>#{@snapshot_version}</version>",
                              "+    <version>#{@release_version}</version>"]],
                     path: "java/flight/flight-core/pom.xml" },
                   { hunks: [["-    <version>#{@snapshot_version}</version>",
                              "+    <version>#{@release_version}</version>"]],
                     path: "java/flight/flight-grpc/pom.xml" },
                   { hunks: [["-  <version>#{@snapshot_version}</version>", "+  <version>#{@release_version}</version>"]],
                     path: "java/format/pom.xml" },
                   { hunks: [["-      <version>#{@snapshot_version}</version>",
                              "+      <version>#{@release_version}</version>"]],
                     path: "java/gandiva/pom.xml" },
                   { hunks: [["-    <version>#{@snapshot_version}</version>",
                              "+    <version>#{@release_version}</version>"]],
                     path: "java/memory/memory-core/pom.xml" },
                   { hunks: [["-    <version>#{@snapshot_version}</version>",
                              "+    <version>#{@release_version}</version>"]],
                     path: "java/memory/memory-netty/pom.xml" },
                   { hunks: [["-    <version>#{@snapshot_version}</version>",
                              "+    <version>#{@release_version}</version>"]],
                     path: "java/memory/memory-unsafe/pom.xml" },
                   { hunks: [["-    <version>#{@snapshot_version}</version>",
                              "+    <version>#{@release_version}</version>"]],
                     path: "java/memory/pom.xml" },
                   { hunks: [["-        <version>#{@snapshot_version}</version>",
                              "+        <version>#{@release_version}</version>"],
                             ["-            <version>#{@snapshot_version}</version>",
                              "+            <version>#{@release_version}</version>"]],
                     path: "java/performance/pom.xml" },
                   { hunks: [["-        <version>#{@snapshot_version}</version>",
                              "+        <version>#{@release_version}</version>"]],
                     path: "java/plasma/pom.xml" },
                   { hunks: [["-  <version>#{@snapshot_version}</version>", "+  <version>#{@release_version}</version>"]],
                     path: "java/pom.xml" },
                   { hunks: [["-        <version>#{@snapshot_version}</version>",
                              "+        <version>#{@release_version}</version>"]],
                     path: "java/tools/pom.xml" },
                   { hunks: [["-    <version>#{@snapshot_version}</version>",
                              "+    <version>#{@release_version}</version>"]],
                     path: "java/vector/pom.xml" },
                   {
                     path: "js/package.json",
                     hunks: [
                       ["-  \"version\": \"#{@snapshot_version}\"",
                        "+  \"version\": \"#{@release_version}\""],
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
                     path: "ruby/red-arrow-dataset/lib/arrow-dataset/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@snapshot_version}\"",
                        "+  VERSION = \"#{@release_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-arrow-flight/lib/arrow-flight/version.rb",
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
                 ],
                 parse_patch(git("log", "-n", "1", "-p")))
  end

  def test_version_post_tag
    if on_release_branch?
      bump_versions("VERSION_POST_TAG")
    else
      prepare("VERSION_PRE_TAG")
      bump_versions("VERSION_POST_TAG")
    end
    assert_equal([
                   {
                     path: "c_glib/meson.build",
                     hunks: [
                       ["-version = '#{@release_version}'",
                        "+version = '#{@next_snapshot_version}'"],
                     ],
                   },
                   {
                     path: "ci/scripts/PKGBUILD",
                     hunks: [
                       ["-pkgver=#{@release_version}",
                        "+pkgver=#{@release_version}.9000"],
                     ],
                   },
                   {
                     path: "cpp/CMakeLists.txt",
                     hunks: [
                       ["-set(ARROW_VERSION \"#{@release_version}\")",
                        "+set(ARROW_VERSION \"#{@next_snapshot_version}\")"],
                     ],
                   },
                   {
                     path: "cpp/vcpkg.json",
                     hunks: [
                       ["-  \"version-string\": \"#{@release_version}\",",
                        "+  \"version-string\": \"#{@next_snapshot_version}\","],
                     ],
                   },
                   {
                     path: "csharp/Directory.Build.props",
                     hunks: [
                       ["-    <Version>#{@release_version}</Version>",
                        "+    <Version>#{@next_snapshot_version}</Version>"],
                     ],
                   },
                   {
                     path: "dev/tasks/homebrew-formulae/apache-arrow.rb",
                     hunks: [
                       ["-  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@release_version}/apache-arrow-#{@release_version}.tar.gz\"",
                        "+  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@next_snapshot_version}/apache-arrow-#{@next_snapshot_version}.tar.gz\""],
                     ],
                   },
                   {
                     path: "dev/tasks/homebrew-formulae/autobrew/apache-arrow.rb",
                     hunks: [
                       ["-  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@release_version}/apache-arrow-#{@release_version}.tar.gz\"",
                        "+  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@release_version}.9000/apache-arrow-#{@release_version}.9000.tar.gz\""],
                     ],
                   },
                   { path: "java/adapter/avro/pom.xml",
                     hunks: [["-    <version>#{@release_version}</version>",
                             "+    <version>#{@next_snapshot_version}</version>"]] },
                   { hunks: [["-        <version>#{@release_version}</version>",
                              "+        <version>#{@next_snapshot_version}</version>"]],
                     path: "java/adapter/jdbc/pom.xml" },
                   { hunks: [["-        <version>#{@release_version}</version>",
                              "+        <version>#{@next_snapshot_version}</version>"]],
                     path: "java/adapter/orc/pom.xml" },
                   { hunks: [["-    <version>#{@release_version}</version>",
                              "+    <version>#{@next_snapshot_version}</version>"]],
                     path: "java/algorithm/pom.xml" },
                   { hunks: [["-    <version>#{@release_version}</version>",
                              "+    <version>#{@next_snapshot_version}</version>"]],
                     path: "java/compression/pom.xml" },
                   { hunks: [["-        <version>#{@release_version}</version>",
                              "+        <version>#{@next_snapshot_version}</version>"]],
                     path: "java/dataset/pom.xml" },
                   { hunks: [["-    <version>#{@release_version}</version>",
                              "+    <version>#{@next_snapshot_version}</version>"]],
                     path: "java/flight/flight-core/pom.xml" },
                   { hunks: [["-    <version>#{@release_version}</version>",
                              "+    <version>#{@next_snapshot_version}</version>"]],
                     path: "java/flight/flight-grpc/pom.xml" },
                   { hunks: [["-  <version>#{@release_version}</version>", "+  <version>#{@next_snapshot_version}</version>"]],
                     path: "java/format/pom.xml" },
                   { hunks: [["-      <version>#{@release_version}</version>",
                              "+      <version>#{@next_snapshot_version}</version>"]],
                     path: "java/gandiva/pom.xml" },
                   { hunks: [["-    <version>#{@release_version}</version>",
                              "+    <version>#{@next_snapshot_version}</version>"]],
                     path: "java/memory/memory-core/pom.xml" },
                   { hunks: [["-    <version>#{@release_version}</version>",
                              "+    <version>#{@next_snapshot_version}</version>"]],
                     path: "java/memory/memory-netty/pom.xml" },
                   { hunks: [["-    <version>#{@release_version}</version>",
                              "+    <version>#{@next_snapshot_version}</version>"]],
                     path: "java/memory/memory-unsafe/pom.xml" },
                   { hunks: [["-    <version>#{@release_version}</version>",
                              "+    <version>#{@next_snapshot_version}</version>"]],
                     path: "java/memory/pom.xml" },
                   { hunks: [["-        <version>#{@release_version}</version>",
                              "+        <version>#{@next_snapshot_version}</version>"],
                             ["-            <version>#{@release_version}</version>",
                              "+            <version>#{@next_snapshot_version}</version>"]],
                     path: "java/performance/pom.xml" },
                   { hunks: [["-        <version>#{@release_version}</version>",
                              "+        <version>#{@next_snapshot_version}</version>"]],
                     path: "java/plasma/pom.xml" },
                   { hunks: [["-  <version>#{@release_version}</version>", "+  <version>#{@next_snapshot_version}</version>"]],
                     path: "java/pom.xml" },
                   { hunks: [["-        <version>#{@release_version}</version>",
                              "+        <version>#{@next_snapshot_version}</version>"]],
                     path: "java/tools/pom.xml" },
                   { hunks: [["-    <version>#{@release_version}</version>",
                              "+    <version>#{@next_snapshot_version}</version>"]],
                     path: "java/vector/pom.xml" },
                   {
                     path: "js/package.json",
                     hunks: [
                       ["-  \"version\": \"#{@release_version}\"",
                        "+  \"version\": \"#{@next_snapshot_version}\""],
                     ],
                   },
                   {
                     path: "matlab/CMakeLists.txt",
                     hunks: [
                       ["-set(MLARROW_VERSION \"#{@release_version}\")",
                        "+set(MLARROW_VERSION \"#{@next_snapshot_version}\")"],
                     ],
                   },
                   {
                     path: "python/setup.py",
                     hunks: [
                       ["-default_version = '#{@release_version}'",
                        "+default_version = '#{@next_snapshot_version}'"],
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
                        "+  VERSION = \"#{@next_snapshot_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-arrow-dataset/lib/arrow-dataset/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_snapshot_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-arrow-flight/lib/arrow-flight/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_snapshot_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-arrow/lib/arrow/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_snapshot_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-gandiva/lib/gandiva/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_snapshot_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-parquet/lib/parquet/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_snapshot_version}\""],
                     ],
                   },
                   {
                     path: "ruby/red-plasma/lib/plasma/version.rb",
                     hunks: [
                       ["-  VERSION = \"#{@release_version}\"",
                        "+  VERSION = \"#{@next_snapshot_version}\""],
                     ],
                   },
                 ],
                 parse_patch(git("log", "-n", "1", "-p")))
  end

  def test_deb_package_names
    bump_versions("DEB_PACKAGE_NAMES")
    changes = parse_patch(git("log", "-n", "1", "-p"))
    sampled_changes = changes.collect do |change|
      first_hunk = change[:hunks][0]
      first_removed_line = first_hunk.find { |line| line.start_with?("-") }
      first_added_line = first_hunk.find { |line| line.start_with?("+") }
      {
        sampled_diff: [first_removed_line, first_added_line],
        path: change[:path],
      }
    end
    expected_changes = [
      {
        sampled_diff: [
          "-dev/tasks/linux-packages/apache-arrow/debian/libarrow-glib#{@so_version}.install",
          "+dev/tasks/linux-packages/apache-arrow/debian/libarrow-glib#{@next_so_version}.install",
        ],
        path: "dev/release/rat_exclude_files.txt",
      },
      {
        sampled_diff: [
          "-Package: libarrow#{@so_version}",
          "+Package: libarrow#{@next_so_version}",
        ],
        path: "dev/tasks/linux-packages/apache-arrow/debian/control.in",
      },
      {
        sampled_diff: [
          "-      - libarrow-dataset-glib#{@so_version}-dbgsym_{no_rc_version}-1_[a-z0-9]+.d?deb",
          "+      - libarrow-dataset-glib#{@next_so_version}-dbgsym_{no_rc_version}-1_[a-z0-9]+.d?deb",
        ],
        path: "dev/tasks/tasks.yml",
      },
    ]
    assert_equal(expected_changes, sampled_changes)
  end
end
