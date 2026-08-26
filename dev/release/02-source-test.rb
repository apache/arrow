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

class SourceTest < Test::Unit::TestCase
  include GitRunnable
  include VersionDetectable

  def setup
    @current_commit = git_current_commit
    detect_versions
    @tag_name_no_rc = "apache-arrow-#{@release_version}"
    @archive_name = "apache-arrow-#{@release_version}.tar.gz"
    @script = File.expand_path("dev/release/02-source.sh")
    @tarball_script = File.expand_path("dev/release/utils-create-release-tarball.sh")
    @env = File.expand_path("dev/release/.env")

    Dir.mktmpdir do |dir|
      Dir.chdir(dir) do
        yield
      end
    end
  end

  def source(*targets)
    env = {
      "SOURCE_DEFAULT" => "0",
      "release_hash" => @current_commit,
    }
    targets.each do |target|
      env["SOURCE_#{target}"] = "1"
    end
    sh(env, @tarball_script, @release_version, "0")
    FileUtils.mkdir_p("artifacts")
    sh("mv", @archive_name, "artifacts/")
    output = sh(env, @script, @release_version, "0")
    sh("tar", "xf", "artifacts/#{@archive_name}")
    output
  end

  def test_symbolic_links
    source
    Dir.chdir(@tag_name_no_rc) do
      assert_equal([],
                   Find.find(".").find_all {|path| File.symlink?(path)})
    end
  end

  def test_python_version
    source
    Dir.chdir("#{@tag_name_no_rc}/python") do
      sh("python", "-m", "build", "--sdist")
      if on_release_branch?
        pyarrow_source_archive = "dist/pyarrow-#{@release_version}.tar.gz"
      else
        pyarrow_source_archive = "dist/pyarrow-#{@release_version}a0.tar.gz"
      end
      assert_equal([pyarrow_source_archive],
                   Dir.glob("dist/pyarrow-*.tar.gz"))
    end
  end
end
