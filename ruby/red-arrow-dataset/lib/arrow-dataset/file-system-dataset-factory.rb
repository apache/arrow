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
  class FileSystemDatasetFactory
    alias_method :set_file_system_uri_raw, :set_file_system_uri
    def set_file_system_uri(uri)
      if uri.is_a?(URI)
        if uri.scheme.nil?
          uri = uri.dup
          absolute_path = File.expand_path(uri.path)
          if absolute_path.start_with?("/")
            uri.path = absolute_path
          else
            uri.path = "/#{absolute_path}"
          end
          uri.scheme = "file"
        end
        uri = uri.to_s
      end
      set_file_system_uri_raw(uri)
    end
    alias_method :file_system_uri=, :set_file_system_uri
  end
end
