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

module ArrowDataset
  module ArrowTableLoadable
    private
    def path_to_uri(path)
      absolute_path = ::File.expand_path(path)
      if absolute_path.start_with?("/")
        URI("file://#{absolute_path}")
      else
        URI("file:///#{absolute_path}")
      end
    end

    def load_from_directory
      internal_load_from_uri(path_to_uri(@input))
    end

    def load_from_uri
      internal_load_from_uri(@input)
    end

    def internal_load_from_uri(uri)
      options = @options.dup
      format = FileFormat.resolve(options.delete(:format))
      dataset = FileSystemDataset.build(format) do |factory|
        factory.file_system_uri = uri
        finish_options = FinishOptions.new
        FinishOptions.instance_methods(false).each do |method|
          next unless method.to_s.end_with?("=")
          value = options.delete(method[0..-2].to_sym)
          next if value.nil?
          finish_options.public_send(method, value)
        end
        finish_options
      end
      scanner_builder = dataset.begin_scan
      options.each do |key, value|
        next if value.nil?
        setter = "#{key}="
        next unless scanner_builder.respond_to?(setter)
        scanner_builder.public_send(setter, value)
      end
      scanner = scanner_builder.finish
      scanner.to_table
    end
  end
end

module Arrow
  class TableLoader
    include ArrowDataset::ArrowTableLoadable
  end
end
