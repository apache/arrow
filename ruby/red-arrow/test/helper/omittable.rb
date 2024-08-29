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
  module Omittable
    def require_ruby(major, minor, micro=0)
      return if (RUBY_VERSION <=> "#{major}.#{minor}.#{micro}") >= 0
      omit("Require Ruby #{major}.#{minor}.#{micro} or later: #{RUBY_VERSION}")
    end

    def require_gi_bindings(major, minor, micro)
      return if GLib.check_binding_version?(major, minor, micro)
      message =
        "Require gobject-introspection #{major}.#{minor}.#{micro} or later: " +
        GLib::BINDING_VERSION.join(".")
      omit(message)
    end

    def require_gi(major, minor, micro)
      return if GObjectIntrospection::Version.or_later?(major, minor, micro)
      message =
        "Require GObject Introspection #{major}.#{minor}.#{micro} or later: " +
        GObjectIntrospection::Version::STRING
      omit(message)
    end
  end
end
