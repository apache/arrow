#!/usr/bin/env ruby
#
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

# This is a one shot https://apache.jfrog.io/ ->
# https://repository.apache.org/ migration tool. The migration is
# already done. So we can remove this anytime.

require "json"
require "rake"

require_relative "binary-task"

maven_args = [nil, nil, ENV["ASF_USER"], ENV["ASF_PASSWORD"]]
maven = BinaryTask::MavenRepositoryClient.new(*maven_args)
repository_id = maven.create_staging_repository

[
  "almalinux",
  "amazon-linux",
  "centos",
  "debian",
  "ubuntu",
].each do |prefix|
  BinaryTask::ArtifactoryClientPool.open(prefix, nil) do |artifactory_pool|
    maven_args = [prefix, repository_id, ENV["ASF_USER"], ENV["ASF_PASSWORD"]]
    BinaryTask::MavenRepositoryClientPool.open(*maven_args) do |maven_pool|
      cache_file = "#{prefix}.list"
      if File.exist?(cache_file)
        files = File.readlines(cache_file, chomp: true)
      else
        files = artifactory_pool.pull do |artifactory|
          artifactory.files
        end
        File.write(cache_file, files.join("\n"))
      end

      progress_reporter =
        BinaryTask::ProgressReporter.new("Copying: #{prefix}", files.size)
      parallel = false
      if parallel
        upload_thread_pool =
          BinaryTask::ThreadPool.new(:maven_repository) do |source, destination|
          maven_pool.pull do |maven|
            maven.upload(source.path, destination)
          end
          progress_reporter.advance
          source.close!
        end
        download_thread_pool = BinaryTask::ThreadPool.new(:artifactory) do |source|
          destination = Tempfile.new("copy-binary")
          File.utime(0, 0, destination.path)
          artifactory_pool.pull do |artifactory|
            artifactory.download(source, destination.path)
          end
          upload_thread_pool << [destination, source]
        end
        files.each do |file|
          download_thread_pool << file
        end
        download_thread_pool.join
        upload_thread_pool.join
      else
        files.each do |file|
          local_file = Tempfile.new("copy-binary")
          File.utime(0, 0, local_file.path)
          artifactory_pool.pull do |artifactory|
            artifactory.download(file, local_file.path)
          end
          maven_pool.pull do |maven|
            maven.upload(local_file.path, file)
          end
          progress_reporter.advance
          local_file.close!
        end
      end
      progress_reporter.finish
    end
  end
end
