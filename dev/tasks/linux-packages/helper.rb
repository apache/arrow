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

module Helper
  module ApacheArrow
    private
    def git_directory?(directory)
      candidate_paths = [".git", "HEAD"]
      candidate_paths.any? do |candidate_path|
        File.exist?(File.join(directory, candidate_path))
      end
    end

    def latest_commit_time(git_directory)
      return nil unless git_directory?(git_directory)
      cd(git_directory) do
        return Time.iso8601(`git log -n 1 --format=%aI`.chomp).utc
      end
    end

    def detect_release_time
      release_time_env = ENV["ARROW_RELEASE_TIME"]
      if release_time_env
        Time.parse(release_time_env).utc
      else
        latest_commit_time(arrow_source_dir) || Time.now.utc
      end
    end

    def arrow_source_dir
      File.join(__dir__, "..", "..", "..")
    end

    def detect_version(release_time)
      version_env = ENV["ARROW_VERSION"]
      return version_env if version_env

      pom_xml_path = File.join(arrow_source_dir, "java", "pom.xml")
      pom_xml_content = File.read(pom_xml_path)
      version = pom_xml_content[/^  <version>(.+?)<\/version>/, 1]
      formatted_release_time = release_time.strftime("%Y%m%d")
      version.gsub(/-SNAPSHOT\z/) {"-dev#{formatted_release_time}"}
    end

    def detect_env(name)
      value = ENV[name]
      return value if value and not value.empty?

      dot_env_path = File.join(arrow_source_dir, ".env")
      File.open(dot_env_path) do |dot_env|
        dot_env.each_line do |line|
          case line.chomp
          when /\A#{Regexp.escape(name)}=(.*)/
            return $1
          end
        end
      end
      raise "Failed to detect #{name} environment variable"
    end

    def detect_repo
      detect_env("REPO")
    end

    def docker_image(os, architecture)
      architecture ||= "amd64"
      "#{detect_repo}:#{architecture}-#{os}-package-#{@package}"
    end
  end
end
