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

# Workflow commands for GitHub Actions:
# https://docs.github.com/en/actions/using-workflows/workflow-commands-for-github-actions

diff_from = nil
diff_to = nil
diff_from_line = nil
diff_to_line = nil
in_diff = false
diff_content = +""

def remove_escape_sequences(text)
  text.gsub(/\e\[.*?m/, "")
end

flush_diff = lambda do
  return unless in_diff

  parameters = [
    "file=#{diff_to}",
    "line=#{diff_to_line[0]}",
    "endLine=#{diff_to_line[1]}",
    "title=diff",
  ].join(",")
  # We need to use URL encode for new line:
  # https://github.com/actions/toolkit/issues/193
  message = remove_escape_sequences(diff_content).gsub("\n", "%0A")
  puts("::error #{parameters}::#{message}")
  in_diff = false
  diff_content = +""
end

ARGF.each_line do |line|
  raw_line = remove_escape_sequences(line)
  case raw_line
  when /\A--- a\/(.+)$/ # git diff
    path = $1
    flush_diff.call
    diff_from = path
    diff_to = nil
    diff_from_line = nil
    diff_to_line = nil
  when /\A?\+\+\+ b\/(.+)$/ # git diff
    diff_to = $1
  when /\A?@@ -(\d+),(\d+) \+(\d+),(\d+) @@/
    diff_from_start = $1.to_i
    diff_from_end = diff_from_start + $2.to_i
    diff_to_start = $3.to_i
    diff_to_end = diff_to_start + $4.to_i
    flush_diff.call
    diff_from_line = [diff_from_start, diff_from_end]
    diff_to_line = [diff_to_start, diff_to_end]
    in_diff = (diff_from and diff_to)
  when /\A[-+ ]/
    diff_content << line if in_diff
  else
    flush_diff.call
  end
  print(line)
end

flush_diff.call
