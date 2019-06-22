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

  def source
    env = {
      "SOURCE_DEFAULT" => "0",
      "release_hash" => @current_commit,
    }
    sh(env, @script, @release_version, "0")
    sh("tar", "xf", "#{@tag_name}.tar.gz")
  end

  def test_git_commit_information
    source
    Dir.chdir("#{@tag_name}/csharp") do
      sh("dotnet", "pack", "-c", "Release")
    end
    Dir.chdir("#{@tag_name}/csharp/artifacts/Apache.Arrow/Release") do
      sh("unzip", "Apache.Arrow.#{@snapshot_version}.nupkg")
      FileUtils.chmod(0400, "Apache.Arrow.nuspec")
      nuspec = REXML::Document.new(File.read("Apache.Arrow.nuspec"))
      nuspec_repository = nuspec.elements["package/metadata/repository"].attributes

      assert_equal([
                    "git",
                    "https://github.com/apache/arrow",
                    "#{@current_commit}"
                   ],
                   [
                     nuspec_repository["type"],
                     nuspec_repository["url"],
                     nuspec_repository["commit"]
                   ])
    end
  end

  def test_source_link_information
    source
    Dir.chdir("#{@tag_name}/csharp") do
      sh("dotnet", "pack", "-c", "Release")

      home = ENV['HOME']
      assert do
        sh("#{home}/.dotnet/tools/sourcelink",
           "test",
           "artifacts/Apache.Arrow/Release/netcoreapp2.1/Apache.Arrow.pdb")
      end
    end
  end

  def test_python_setup
    source
    Dir.chdir("#{@tag_name}/python") do
      sh("python3", "setup.py", "sdist")
      assert_equal(["dist/pyarrow-#{@release_version}a0.tar.gz"],
                   Dir.glob("dist/pyarrow-*.tar.gz"))
    end
  end
end
