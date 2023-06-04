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

module ArrowFlight
  class Client
    # Authenticates by Basic authentication.
    #
    # @param user [String] User name to be used.
    # @param password [String] Password to be used.
    # @param options [ArrowFlight::CallOptions, Hash, nil] (nil)
    #   The options to be used.
    #
    # @return [ArrowFlight::CallOptions] The options that can be used
    #   for following calls. It includes Bearer token for @user.
    #
    #   If @options is an ArrowFlight::CallOptions, the given @options
    #   is returned with Bearer token.
    #
    #   If @options isn't an ArrowFlight::CallOptions, a new
    #   ArrowFlight::CallOptions is created and it's returned.
    #
    # @since 13.0.0
    def authenticate_basic(user, password, options=nil)
      unless options.is_a?(CallOptions)
        options = CallOptions.try_convert(options)
      end
      options ||= CallOptions.new
      _success, bearer_name, bearer_value =
        authenticate_basic_token(user, password, options)
      invalid_bearer = (bearer_name.empty? or bearer_value.empty?)
      unless invalid_bearer
        options.add_header(bearer_name, bearer_value)
      end
      options
    end
  end
end
