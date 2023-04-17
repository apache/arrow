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
      if on_release_branch?
        pyarrow_source_archive = "dist/pyarrow-#{@release_version}.tar.gz"
      else
        pyarrow_source_archive = "dist/pyarrow-#{@release_version}a0.tar.gz"
      end
      assert_equal([pyarrow_source_archive],
                   Dir.glob("dist/pyarrow-*.tar.gz"))
    end
  end

  def test_vote
    github_token = ENV["ARROW_GITHUB_API_TOKEN"]
    uri = URI.parse("https://api.github.com/graphql")
    n_issues_query = {
      "query" => <<-QUERY,
        query {
          search(query: "repo:apache/arrow is:issue is:closed milestone:#{@release_version}",
                 type: ISSUE) {
            issueCount
          }
        }
      QUERY
    }
    response = Net::HTTP.post(uri,
                              n_issues_query.to_json,
                              "Content-Type" => "application/json",
                              "Authorization" => "Bearer #{github_token}")
    n_resolved_issues = JSON.parse(response.body)["data"]["search"]["issueCount"]
    github_api_url = "https://api.github.com"
    verify_prs = URI("#{github_api_url}/repos/apache/arrow/pulls" +
                     "?state=open" +
                     "&head=apache:release-#{@release_version}-rc0")
    verify_pr_url = nil
    headers = {
      "Accept" => "application/vnd.github+json",
    }

    if github_token
      headers["Authorization"] = "Bearer #{github_token}"
    end
    verify_prs.open(headers) do |response|
      verify_pr_url = (JSON.parse(response.read)[0] || {})["html_url"]
    end
    output = source("VOTE")
    assert_equal(<<-VOTE.strip, output[/^-+$(.+?)^-+$/m, 1].strip)
To: dev@arrow.apache.org
Subject: [VOTE] Release Apache Arrow #{@release_version} - RC0

Hi,

I would like to propose the following release candidate (RC0) of Apache
Arrow version #{@release_version}. This is a release consisting of #{n_resolved_issues}
resolved JIRA issues[1].

This release candidate is based on commit:
#{@current_commit} [2]

The source release rc0 is hosted at [3].
The binary artifacts are hosted at [4][5][6][7][8][9][10][11].
The changelog is located at [12].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release. See [13] for how to validate a release candidate.

See also a verification result on GitHub pull request [14].

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow #{@release_version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow #{@release_version} because...

[1]: https://github.com/apache/arrow/issues?q=is%3Aissue+milestone%3A#{@release_version}+is%3Aclosed
[2]: https://github.com/apache/arrow/tree/#{@current_commit}
[3]: https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-#{@release_version}-rc0
[4]: https://apache.jfrog.io/artifactory/arrow/almalinux-rc/
[5]: https://apache.jfrog.io/artifactory/arrow/amazon-linux-rc/
[6]: https://apache.jfrog.io/artifactory/arrow/centos-rc/
[7]: https://apache.jfrog.io/artifactory/arrow/debian-rc/
[8]: https://apache.jfrog.io/artifactory/arrow/java-rc/#{@release_version}-rc0
[9]: https://apache.jfrog.io/artifactory/arrow/nuget-rc/#{@release_version}-rc0
[10]: https://apache.jfrog.io/artifactory/arrow/python-rc/#{@release_version}-rc0
[11]: https://apache.jfrog.io/artifactory/arrow/ubuntu-rc/
[12]: https://github.com/apache/arrow/blob/#{@current_commit}/CHANGELOG.md
[13]: https://cwiki.apache.org/confluence/display/ARROW/How+to+Verify+Release+Candidates
[14]: #{verify_pr_url || "null"}
    VOTE
  end
end
