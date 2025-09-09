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

class PostBumpVersionsTest < Test::Unit::TestCase
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
        unless git_tags.include?("apache-arrow-#{@release_version}")
          git("tag", "apache-arrow-#{@release_version}")
        end
        yield
      end
      FileUtils.rm_rf(@test_git_repository)
    end
  end

  def bump_type
    (data || {})[:bump_type]
  end

  def released_version
    return @release_version if bump_type.nil?

    previous_version_components = @previous_version.split(".")
    case bump_type
    when :minor
      previous_version_components[1].succ!
    when :patch
      previous_version_components[2].succ!
    end
    previous_version_components.join(".")
  end

  def released_compatible_version
    compute_compatible_version(released_version)
  end

  def bump_versions(*targets)
    if targets.last.is_a?(Hash)
      additional_env = targets.pop
    else
      additional_env = {}
    end
    env = {"BUMP_DEFAULT" => "0"}
    targets.each do |target|
      env["BUMP_#{target}"] = "1"
    end
    env = env.merge(additional_env)
    case bump_type
    when :minor, :patch
      sh(env,
         "dev/release/post-10-bump-versions.sh",
         released_version,
         @release_version)
    else
      sh(env,
         "dev/release/post-10-bump-versions.sh",
         released_version,
         @next_version)
    end
  end

  data("X.0.0 -> X.0.1",      {next_release_type: :patch})
  data("X.0.0 -> X.1.0",      {next_release_type: :minor})
  data("X.0.0 -> ${X+1}.0.0", {next_release_type: :major})
  data("X.0.1 -> ${X+1}.0.0", {bump_type: :patch})
  data("X.1.0 -> ${X+1}.0.0", {bump_type: :minor})
  def test_version_post_tag
    omit_on_release_branch

    case bump_type
    when :patch, :minor
      expected_changes = [
        {
          path: "ci/scripts/PKGBUILD",
          hunks: [
            ["-pkgver=#{@previous_version}.9000",
             "+pkgver=#{released_version}.9000"],
          ],
        },
      ]
      if bump_type == :minor
        expected_changes += [
          {
            path: "docs/source/_static/versions.json",
            hunks: [
              [
                "-        \"name\": \"#{@previous_compatible_version} (stable)\",",
                "+        \"name\": \"#{released_compatible_version} (stable)\",",
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
          path: "r/DESCRIPTION",
          hunks: [
            ["-Version: #{@previous_version}.9000",
             "+Version: #{released_version}.9000"],
          ],
        },
        {
          path: "r/NEWS.md",
          hunks: [
            ["-# arrow #{@previous_version}.9000",
             "+# arrow #{released_version}.9000",
             "+",
             "+# arrow #{released_version}",],
          ],
        },
        {
          path: "r/pkgdown/assets/versions.html",
          hunks: [
            [
              "-<body><p><a href=\"../dev/r/\">#{@previous_version}.9000 (dev)</a></p>",
              "-<p><a href=\"../r/\">#{@previous_r_version} (release)</a></p>",
              "+<body><p><a href=\"../dev/r/\">#{released_version}.9000 (dev)</a></p>",
              "+<p><a href=\"../r/\">#{released_version} (release)</a></p>",
            ],
          ],
        },
        {
          path: "r/pkgdown/assets/versions.json",
          hunks: [
            [
              "-        \"name\": \"#{@previous_version}.9000 (dev)\",",
              "+        \"name\": \"#{released_version}.9000 (dev)\",",
              "-        \"name\": \"#{@previous_r_version} (release)\",",
              "+        \"name\": \"#{released_version} (release)\",",
            ],
          ],
        },
      ]
    else
      expected_changes = [
        {
          path: "c_glib/meson.build",
          hunks: [
            ["-    version: '#{@snapshot_version}',",
             "+    version: '#{@next_snapshot_version}',"],
          ],
        },
        {
          path: "c_glib/vcpkg.json",
          hunks: [
            ["-  \"version-string\": \"#{@snapshot_version}\",",
             "+  \"version-string\": \"#{@next_snapshot_version}\","],
          ],
        },
        {
          path: "ci/scripts/PKGBUILD",
          hunks: [
            ["-pkgver=#{@previous_version}.9000",
             "+pkgver=#{@release_version}.9000"],
          ],
        },
        {
          path: "cpp/CMakeLists.txt",
          hunks: [
            ["-set(ARROW_VERSION \"#{@snapshot_version}\")",
             "+set(ARROW_VERSION \"#{@next_snapshot_version}\")"],
          ],
        },
        {
          path: "cpp/meson.build",
          hunks: [
            ["-    version: '#{@snapshot_version}',",
             "+    version: '#{@next_snapshot_version}',"],
          ],
        },
        {
          path: "cpp/vcpkg.json",
          hunks: [
            ["-  \"version-string\": \"#{@snapshot_version}\",",
             "+  \"version-string\": \"#{@next_snapshot_version}\","],
          ],
        },
        {
          path: "csharp/Directory.Build.props",
          hunks: [
            ["-    <Version>#{@snapshot_version}</Version>",
             "+    <Version>#{@next_snapshot_version}</Version>"],
          ],
        },
        {
          path: "dev/tasks/homebrew-formulae/apache-arrow-glib.rb",
          hunks: [
            ["-  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@snapshot_version}/apache-arrow-#{@snapshot_version}.tar.gz\"",
             "+  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@next_snapshot_version}/apache-arrow-#{@next_snapshot_version}.tar.gz\""],
          ],
        },
        {
          path: "dev/tasks/homebrew-formulae/apache-arrow.rb",
          hunks: [
            ["-  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@snapshot_version}/apache-arrow-#{@snapshot_version}.tar.gz\"",
             "+  url \"https://www.apache.org/dyn/closer.lua?path=arrow/arrow-#{@next_snapshot_version}/apache-arrow-#{@next_snapshot_version}.tar.gz\""],
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
             "+set(MLARROW_VERSION \"#{@next_snapshot_version}\")"],
          ],
        },
        {
          path: "python/CMakeLists.txt",
          hunks: [
            ["-set(PYARROW_VERSION \"#{@snapshot_version}\")",
             "+set(PYARROW_VERSION \"#{@next_snapshot_version}\")"],
          ],
        },
        {
          path: "python/pyproject.toml",
          hunks: [
            ["-fallback_version = '#{@release_version}a0'",
             "+fallback_version = '#{@next_version}a0'"],
          ],
        },
        {
          path: "r/DESCRIPTION",
          hunks: [
            ["-Version: #{@previous_version}.9000",
             "+Version: #{@release_version}.9000"],
          ],
        },
        {
          path: "r/NEWS.md",
          hunks: [
            ["-# arrow #{@previous_version}.9000",
             "+# arrow #{@release_version}.9000",
             "+",
             "+# arrow #{@release_version}",],
          ],
        },
      ]
      if next_release_type == :major
        expected_changes += [
          {
            path: "c_glib/tool/generate-version-header.py",
            hunks: [
              ["+    (#{@next_major_version}, 0),"],
            ],
          },
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
              ],
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
              ],
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
              ],
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
              ],
            ],
          },
        ]
      end

      Dir.glob("ruby/**/version.rb") do |path|
        version = "  VERSION = \"#{@snapshot_version}\""
        new_version = "  VERSION = \"#{@next_snapshot_version}\""
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
    end

    stdout = bump_versions("VERSION_POST_TAG")
    assert_equal(expected_changes.sort_by {|diff| diff[:path]},
                 parse_patch(git("log", "-n", "1", "-p")),
                 "Output:\n#{stdout}")
  end

  data(:bump_type, [nil, :minor, :patch])
  def test_deb_package_names
    omit_on_release_branch unless bump_type.nil?
    current_commit = git_current_commit
    stdout = bump_versions("VERSION_POST_TAG", "DEB_PACKAGE_NAMES")
    log = git("log", "-p", "#{current_commit}..")
    # Remove a commit for VERSION_POST_TAG
    if log.scan(/^commit/).size == 1
      log = ""
    else
      log.gsub!(/\A(commit.*?)^commit .*\z/um, "\\1")
    end
    changes = parse_patch(log)
    sampled_changes = changes.collect do |change|
      first_hunk = change[:hunks][0]
      first_removed_line = first_hunk.find { |line| line.start_with?("-") }
      first_added_line = first_hunk.find { |line| line.start_with?("+") }
      {
        sampled_diff: [first_removed_line, first_added_line],
        path: change[:path],
      }
    end
    if bump_type.nil?
      expected_changes = [
        {
          sampled_diff: [
            "-Package: libarrow#{@so_version}",
            "+Package: libarrow#{@next_so_version}",
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
    name = "Arrow Developers"
    email = "dev@arrow.apache.org"
    stdout = bump_versions("LINUX_PACKAGES",
                           "DEBFULLNAME" => name,
                           "DEBEMAIL" => email)

    release_time_string = git("log",
                              "--format=%aI",
                              "-n", "1",
                              "apache-arrow-#{@release_version}")
    release_time = Time.iso8601(release_time_string).utc
    deb_packager_info = "#{name} <#{email}>  #{release_time.rfc2822}"
    deb_package_metadata = "(#{@release_version}-1) unstable; urgency=low"
    rpm_release_time_string = release_time.strftime("%a %b %d %Y")
    rpm_packager_info = "#{rpm_release_time_string} #{name} <#{email}>"
    expected_changes = [
      {
        hunks: [
          [
            "+apache-arrow-apt-source #{deb_package_metadata}",
            "+",
            "+  * New upstream release.",
            "+",
            "+ -- #{deb_packager_info}",
            "+",
          ],
        ],
        path: "dev/tasks/linux-packages/apache-arrow-apt-source/debian/changelog",
      },
      {
        hunks: [
          [
            "+* #{rpm_packager_info} - #{@release_version}-1",
            "+- New upstream release.",
            "+",
          ],
        ],
        path: "dev/tasks/linux-packages/apache-arrow-release/yum/apache-arrow-release.spec.in",
      },
      {
        hunks: [
          [
            "+apache-arrow #{deb_package_metadata}",
            "+",
            "+  * New upstream release.",
            "+",
            "+ -- #{deb_packager_info}",
            "+",
          ],
        ],
        path: "dev/tasks/linux-packages/apache-arrow/debian/changelog",
      },
      {
        hunks: [
          [
            "+* #{rpm_packager_info} - #{@release_version}-1",
            "+- New upstream release.",
            "+",
          ],
        ],
        path: "dev/tasks/linux-packages/apache-arrow/yum/arrow.spec.in",
      },
    ]
    assert_equal(expected_changes,
                 parse_patch(git("log", "-n", "1", "-p")),
                 "Output:\n#{stdout}")
  end
end
