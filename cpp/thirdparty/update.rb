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

require "digest/sha2"
require "json"
require "open-uri"
require "optparse"

option_parser = OptionParser.new
option_parser.banner =
  "Usage: #{$0} [options] PRODUCT_PATTERN1 PRODUCT_PATTERN2 ..."
patterns = option_parser.parse!(ARGV)
if patterns.empty?
  puts(option_parser)
  exit(false)
end

def parse_versions_txt_content(content)
  products = {}
  content.each_line(chomp: true) do |line|
    case line
    when /\AARROW_([A-Za-z0-9_-]+)_BUILD_VERSION=(.+?)\z/
      product = Regexp.last_match[1]
      version = Regexp.last_match[2]
      products[product] = {version: version}
    when /\AARROW_([A-Za-z0-9_-]+)_BUILD_SHA256_CHECKSUM=(.+?)\z/
      product = Regexp.last_match[1]
      checksum = Regexp.last_match[2]
      products[product][:checksum] = checksum
    when /\A  "ARROW_([A-Za-z0-9_-]+)_URL (?:\S+) (\S+)"\z/
      product = Regexp.last_match[1]
      url_template = Regexp.last_match[2]
      url_template.gsub!(/\${.+?}/) do |matched|
        if matched.end_with?("//./_}")
          "%{version_underscore}"
        else
          "%{version}"
        end
      end
      products[product][:url_template] = url_template
    end
  end
  products
end

def update_product_github(product, metadata, repository)
  version = metadata[:version]
  tags_url = "https://api.github.com/repos/#{repository}/tags"
  tags = URI.open(tags_url) do |response|
    JSON.parse(response.read)
  end
  latest_tag_name = tags[0]["name"]
  if latest_tag_name.start_with?("v")
    if metadata[:version].start_with?("v")
      latest_version = latest_tag_name
    else
      latest_version = latest_tag_name[1..-1]
    end
  else
    latest_version = latest_tag_name
  end
  return if version == latest_version

  url_template = metadata[:url_template]
  url = url_template % {
    version: latest_version,
    version_underscore: latest_version.gsub(".", "_"),
  }
  $stderr.puts("Updating #{product}: #{version} -> #{latest_version}")
  metadata[:version] = latest_version
  URI.open(url, "rb") do |response|
    metadata[:checksum] = Digest::SHA256.hexdigest(response.read)
  end
  $stderr.puts("  Checksum: #{metadata[:checksum]}")
end

def update_product(product, metadata)
  url_template = metadata[:url_template]
  if url_template.nil?
    $stderr.puts("#{product} isn't supported " +
                 "because there is no associated URL")
    return
  end

  case url_template
  when /\Ahttps:\/\/github.com\/((?:[^\/]+)\/(?:[^\/]+))\//
    github_repository = Regexp.last_match[1]
    update_product_github(product, metadata, github_repository)
  else
    $stderr.puts("TODO: #{product} isn't supported yet: #{url_template}")
  end
end

def update_versions_txt_content!(content, products)
  products.each do |product, metadata|
    prefix = "ARROW_#{Regexp.escape(product)}"
    content.gsub!(/^#{prefix}_BUILD_VERSION=.*$/) do
      "ARROW_#{product}_BUILD_VERSION=#{metadata[:version]}"
    end
    content.gsub!(/^#{prefix}_BUILD_SHA256_CHECKSUM=.*?$/) do
      "ARROW_#{product}_BUILD_SHA256_CHECKSUM=#{metadata[:checksum]}"
    end
  end
end

versions_txt = File.join(__dir__, "versions.txt")
versions_txt_content = File.read(versions_txt)
products = parse_versions_txt_content(versions_txt_content)
patterns.each do |pattern|
  target_products = products.filter do |product, _|
    File.fnmatch?(pattern, product)
  end
  target_products.each do |product, metadata|
    update_product(product, metadata)
  end
end
update_versions_txt_content!(versions_txt_content, products)
File.write(versions_txt, versions_txt_content)
