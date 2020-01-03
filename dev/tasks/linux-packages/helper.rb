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
      version_env = ENV['ARROW_VERSION']
      return version_env if version_env

      pom_xml_path = File.join(arrow_source_dir, "java", "pom.xml")
      pom_xml_content = File.read(pom_xml_path)
      version = pom_xml_content[/^  <version>(.+?)<\/version>/, 1]
      formatted_release_time = release_time.strftime("%Y%m%d")
      version.gsub(/-SNAPSHOT\z/) {"-dev#{formatted_release_time}"}
    end
  end
end
