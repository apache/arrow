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
    output = sh(env, @script, @release_version, "0")
    sh("tar", "xf", @archive_name)
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
    github_token = File.read(@env)[/^GH_TOKEN=(.*)$/, 1]
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
    github_repository = ENV["GITHUB_REPOSITORY"] || "apache/arrow"
    github_api_url = "https://api.github.com"
    verify_prs = URI("#{github_api_url}/repos/#{github_repository}/pulls" +
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
resolved GitHub issues[1].

This release candidate is based on commit:
#{@current_commit} [2]

The source release rc0 is hosted at [3].
The binary artifacts are hosted at [4][5][6][7][8][9].
The changelog is located at [10].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release. See [11] for how to validate a release candidate.

See also a verification result on GitHub pull request [12].

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow #{@release_version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow #{@release_version} because...

[1]: https://github.com/apache/arrow/issues?q=is%3Aissue+milestone%3A#{@release_version}+is%3Aclosed
[2]: https://github.com/apache/arrow/tree/#{@current_commit}
[3]: https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-#{@release_version}-rc0
[4]: https://packages.apache.org/artifactory/arrow/almalinux-rc/
[5]: https://packages.apache.org/artifactory/arrow/amazon-linux-rc/
[6]: https://packages.apache.org/artifactory/arrow/centos-rc/
[7]: https://packages.apache.org/artifactory/arrow/debian-rc/
[8]: https://packages.apache.org/artifactory/arrow/ubuntu-rc/
[9]: https://github.com/apache/arrow/releases/tag/apache-arrow-#{@release_version}-rc0
[10]: https://github.com/apache/arrow/blob/#{@current_commit}/CHANGELOG.md
[11]: https://arrow.apache.org/docs/developers/release_verification.html
[12]: #{verify_pr_url || "null"}
    VOTE
  end
end
