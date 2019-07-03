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

require "rexml/document"

class SourceTest < Test::Unit::TestCase
  include GitRunnable
  include VersionDetectable

  def setup
    @current_commit = git_current_commit
    detect_versions
    @tag_name = "apache-arrow-#{@release_version}"
    @script = File.expand_path("dev/release/02-source.sh")

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
    sh(env, @script, @release_version, "0")
    sh("tar", "xf", "#{@tag_name}.tar.gz")
  end

  def test_symbolic_links
    source
    Dir.chdir("#{@tag_name}") do
      assert_equal([],
                   Find.find(".").find_all {|path| File.symlink?(path)})
    end
  end

  def test_glib_configure
    source("GLIB")
    Dir.chdir("#{@tag_name}/c_glib") do
      assert_equal([
                     "configure",
                     "configure.ac",
                   ],
                   Dir.glob("configure*").sort)
    end
  end

  def test_csharp_git_commit_information
    source
    Dir.chdir("#{@tag_name}/csharp") do
      FileUtils.mv("dummy.git", "../.git")
      sh("dotnet", "pack", "-c", "Release")
      FileUtils.mv("../.git", "dummy.git")
      Dir.chdir("artifacts/Apache.Arrow/Release") do
        sh("unzip", "Apache.Arrow.#{@snapshot_version}.nupkg")
        FileUtils.chmod(0400, "Apache.Arrow.nuspec")
        nuspec = REXML::Document.new(File.read("Apache.Arrow.nuspec"))
        nuspec_repository = nuspec.elements["package/metadata/repository"]
        attributes = {}
        nuspec_repository.attributes.each do |key, value|
          attributes[key] = value
        end
        assert_equal({
                       "type" => "git",
                       "url" => "https://github.com/apache/arrow",
                       "commit" => @current_commit,
                     },
                     attributes)
      end
    end
  end

  def test_python_version
    source
    Dir.chdir("#{@tag_name}/python") do
      sh("python3", "setup.py", "sdist")
      assert_equal(["dist/pyarrow-#{@release_version}a0.tar.gz"],
                   Dir.glob("dist/pyarrow-*.tar.gz"))
    end
  end
end
