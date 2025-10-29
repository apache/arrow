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
      FileUtils.cp((top_dir + "dev" + "release" + ".env").to_s,
                   (@test_git_repository + "dev" + "release").to_s)
      Dir.chdir(@test_git_repository) do
        @release_branch = "testing-release-#{@release_version}-rc0"
        git("checkout", "-b", @release_branch, @current_commit)
        yield
      end
      FileUtils.rm_rf(@test_git_repository)
    end
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

  data(:release_type, [nil, :major, :minor, :patch])
  def test_deb_package_names
    omit_on_release_branch
    current_commit = git_current_commit
    stdout = prepare("DEB_PACKAGE_NAMES")
    changes = parse_patch(git("log", "-p", "#{current_commit}.."))
    sampled_changes = changes.collect do |change|
      first_hunk = change[:hunks][0]
      first_removed_line = first_hunk.find { |line| line.start_with?("-") }
      first_added_line = first_hunk.find { |line| line.start_with?("+") }
      {
        sampled_diff: [first_removed_line, first_added_line],
        path: change[:path],
      }
    end
    case release_type
    when :major, :minor
      expected_changes = [
        {
          sampled_diff: [
            "-Package: libarrow#{@snapshot_so_version}",
            "+Package: libarrow#{@so_version}",
          ],
          path: "dev/tasks/linux-packages/apache-arrow/debian/control.in",
        },
      ]
    else
      expected_changes = []
    end
    assert_equal(expected_changes, sampled_changes, "Output:\n#{stdout}")
  end

  def test_linux_packages
    user = "Arrow Developers"
    email = "dev@arrow.apache.org"
    stdout = prepare("LINUX_PACKAGES",
                     "DEBFULLNAME" => user,
                     "DEBEMAIL" => email)
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
    assert_equal(expected_changes, sampled_changes, "Output:\n#{stdout}")
  end

  data(:next_release_type, [:major, :minor, :patch])
  def test_version_pre_tag
    omit_on_release_branch

    expected_changes = [
      {
        path: "c_glib/meson.build",
        hunks: [
          ["-    version: '#{@snapshot_version}',",
           "+    version: '#{@release_version}',"],
        ],
      },
      {
        path: "c_glib/vcpkg.json",
        hunks: [
          ["-  \"version-string\": \"#{@snapshot_version}\",",
           "+  \"version-string\": \"#{@release_version}\","],
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
        path: "cpp/meson.build",
        hunks: [
          ["-    version: '#{@snapshot_version}',",
           "+    version: '#{@release_version}',"],
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
        path: "dev/tasks/homebrew-formulae/apache-arrow-glib.rb",
        hunks: [
          ["-  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@snapshot_version}/apache-arrow-#{@snapshot_version}.tar.gz\"",
           "+  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@release_version}/apache-arrow-#{@release_version}.tar.gz\""],
        ],
      },
      {
        path: "dev/tasks/homebrew-formulae/apache-arrow.rb",
        hunks: [
          ["-  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@snapshot_version}/apache-arrow-#{@snapshot_version}.tar.gz\"",
           "+  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@release_version}/apache-arrow-#{@release_version}.tar.gz\""],
        ],
      },
    ]
    unless next_release_type == :patch
      expected_changes += [
        {
          path: "docs/source/_static/versions.json",
          hunks: [
            [
              "-        \"name\": \"#{@release_compatible_version} (dev)\",",
              "+        \"name\": \"#{@next_compatible_version} (dev)\",",
              "-        \"name\": \"#{@previous_compatible_version} (stable)\",",
              "+        \"name\": \"#{@release_compatible_version} (stable)\",",
              "+    {",
              "+        \"name\": \"#{@previous_compatible_version}\",",
              "+        \"version\": \"#{@previous_compatible_version}/\",",
              "+        \"url\": \"https://arrow.apache.org/docs/#{@previous_compatible_version}/\"",
              "+    },",
            ],
          ],
        },
      ]
    end
    expected_changes += [
      {
        path: "matlab/CMakeLists.txt",
        hunks: [
          ["-set(MLARROW_VERSION \"#{@snapshot_version}\")",
           "+set(MLARROW_VERSION \"#{@release_version}\")"],
        ],
      },
      {
        path: "python/CMakeLists.txt",
        hunks: [
          ["-set(PYARROW_VERSION \"#{@snapshot_version}\")",
           "+set(PYARROW_VERSION \"#{@release_version}\")"],
        ],
      },
      {
        path: "python/pyproject.toml",
        hunks: [
          ["-fallback_version = '#{@release_version}a0'",
           "+fallback_version = '#{@release_version}'"],
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
    ]
    if next_release_type == :major
      expected_changes += [
        {
          path: "r/pkgdown/assets/versions.html",
          hunks: [
            [
              "-<body><p><a href=\"../dev/r/\">#{@previous_version}.9000 (dev)</a></p>",
              "-<p><a href=\"../r/\">#{@previous_r_version} (release)</a></p>",
              "+<body><p><a href=\"../dev/r/\">#{@release_version}.9000 (dev)</a></p>",
              "+<p><a href=\"../r/\">#{@release_version} (release)</a></p>",
              "+<p><a href=\"../#{@previous_compatible_version}/r/\">" +
              "#{@previous_r_version}</a></p>",
            ]
          ],
        },
        {
          path: "r/pkgdown/assets/versions.json",
          hunks: [
            [
              "-        \"name\": \"#{@previous_version}.9000 (dev)\",",
              "+        \"name\": \"#{@release_version}.9000 (dev)\",",
              "-        \"name\": \"#{@previous_r_version} (release)\",",
              "+        \"name\": \"#{@release_version} (release)\",",
              "+    {",
              "+        \"name\": \"#{@previous_r_version}\",",
              "+        \"version\": \"#{@previous_compatible_version}/\"",
              "+    },",
            ]
          ],
        },
      ]
    else
      expected_changes += [
        {
          path: "r/pkgdown/assets/versions.html",
          hunks: [
            [
              "-<body><p><a href=\"../dev/r/\">#{@previous_version}.9000 (dev)</a></p>",
              "-<p><a href=\"../r/\">#{@previous_r_version} (release)</a></p>",
              "+<body><p><a href=\"../dev/r/\">#{@release_version}.9000 (dev)</a></p>",
              "+<p><a href=\"../r/\">#{@release_version} (release)</a></p>",
            ]
          ],
        },
        {
          path: "r/pkgdown/assets/versions.json",
          hunks: [
            [
              "-        \"name\": \"#{@previous_version}.9000 (dev)\",",
              "+        \"name\": \"#{@release_version}.9000 (dev)\",",
              "-        \"name\": \"#{@previous_r_version} (release)\",",
              "+        \"name\": \"#{@release_version} (release)\",",
            ]
          ],
        },
      ]
    end

    Dir.glob("ruby/**/version.rb") do |path|
      version = "  VERSION = \"#{@snapshot_version}\""
      new_version = "  VERSION = \"#{@release_version}\""
      expected_changes << {
        hunks: [
          [
            "-#{version}",
            "+#{new_version}",
          ]
        ],
        path: path,
      }
    end

    stdout = prepare("VERSION_PRE_TAG")
    assert_equal(expected_changes.sort_by {|diff| diff[:path]},
                 parse_patch(git("log", "-n", "1", "-p")),
                 "Output:\n#{stdout}")
  end
end
