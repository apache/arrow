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
    output = sh(env, @script, @release_version, "0")
    sh("tar", "xf", "#{@tag_name}.tar.gz")
    output
  end

  def test_symbolic_links
    source
    Dir.chdir(@tag_name) do
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

  def test_vote
    jira_url = "https://issues.apache.org/jira"
    jql_conditions = [
      "project = ARROW",
      "status in (Resolved, Closed)",
      "fixVersion = #{@release_version}",
    ]
    jql = jql_conditions.join(" AND ")
    n_resolved_issues = nil
    open("#{jira_url}/rest/api/2/search?jql=#{CGI.escape(jql)}") do |response|
      n_resolved_issues = JSON.parse(response.read)["total"]
    end
    output = source("VOTE")
    assert_equal(<<-VOTE.strip, output[/^-+$(.+?)^-+$/m, 1].strip)
To: dev@arrow.apache.org
Subject: [VOTE] Release Apache Arrow #{@release_version} - RC0

Hi,

I would like to propose the following release candidate (RC0) of Apache
Arrow version #{@release_version}. This is a release consiting of #{n_resolved_issues}
resolved JIRA issues[1].

This release candidate is based on commit:
#{@current_commit} [2]

The source release rc0 is hosted at [3].
The binary artifacts are hosted at [4][5][6][7].
The changelog is located at [8].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release. See [9] for how to validate a release candidate.

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow #{@release_version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow #{@release_version} because...

[1]: https://issues.apache.org/jira/issues/?jql=project%20%3D%20ARROW%20AND%20status%20in%20%28Resolved%2C%20Closed%29%20AND%20fixVersion%20%3D%20#{@release_version}
[2]: https://github.com/apache/arrow/tree/#{@current_commit}
[3]: https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-#{@release_version}-rc0
[4]: https://bintray.com/apache/arrow/centos-rc/#{@release_version}-rc0
[5]: https://bintray.com/apache/arrow/debian-rc/#{@release_version}-rc0
[6]: https://bintray.com/apache/arrow/python-rc/#{@release_version}-rc0
[7]: https://bintray.com/apache/arrow/ubuntu-rc/#{@release_version}-rc0
[8]: https://github.com/apache/arrow/blob/#{@current_commit}/CHANGELOG.md
[9]: https://cwiki.apache.org/confluence/display/ARROW/How+to+Verify+Release+Candidates
    VOTE
  end
end
