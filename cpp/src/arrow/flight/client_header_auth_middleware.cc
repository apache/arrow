// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Interfaces for defining middleware for Flight clients. Currently
// experimental.

#include "client_header_auth_middleware.h"
#include "client_middleware.h"
#include "client_auth.h"
#include "client.h"

namespace arrow {
namespace flight {

  std::string base64_encode(const std::string& input);

  ClientBearerTokenMiddleware::ClientBearerTokenMiddleware(
    std::pair<std::string, std::string>* bearer_token_)
        : bearer_token(bearer_token_) { }

  void ClientBearerTokenMiddleware::SendingHeaders(AddCallHeaders* outgoing_headers) { }

  void ClientBearerTokenMiddleware::ReceivedHeaders(
    const CallHeaders& incoming_headers) {
    // Grab the auth token if one exists.
    auto bearer_iter = incoming_headers.find(AUTH_HEADER);
    if (bearer_iter == incoming_headers.end()) {
      return;
    }

    // Check if the value of the auth token starts with the bearer prefix, latch the token.
    std::string bearer_val = bearer_iter->second.to_string();
    if (bearer_val.size() > BEARER_PREFIX.size()) {
      bool hasPrefix = std::equal(bearer_val.begin(), bearer_val.begin() + BEARER_PREFIX.size(), BEARER_PREFIX.begin(),
        [] (const char& char1, const char& char2) {
          return (std::toupper(char1) == std::toupper(char2));
        }
      );
      if (hasPrefix) {
        *bearer_token = std::make_pair(AUTH_HEADER, bearer_val);
      }
    }
  }

  void ClientBearerTokenMiddleware::CallCompleted(const Status& status) { }

  void ClientBearerTokenFactory::StartCall(const CallInfo& info, std::unique_ptr<ClientMiddleware>* middleware) {
    *middleware = std::unique_ptr<ClientBearerTokenMiddleware>(new ClientBearerTokenMiddleware(bearer_token));
  }

  void ClientBearerTokenFactory::Reset() {
    *bearer_token = std::make_pair("", "");
  }

  template<typename ... Args>
  std::string string_format(const std::string& format, const Args... args) {
    // Check size requirement for new string and increment by 1 for null terminator.
    size_t size = std::snprintf(nullptr, 0, format.c_str(), args ...) + 1;
    if(size <= 0){
      throw std::runtime_error("Error during string formatting. Format: '" + format + "'.");
    }

    // Create buffer for new string and write string in.
    std::unique_ptr<char[]> buf(new char[size]);
    std::snprintf(buf.get(), size, format.c_str(), args...);

    // Convert to std::string, subtracting size by 1 to trim null terminator.
    return std::string(buf.get(), buf.get() + size - 1);
  }

  void AddBasicAuthHeaders(grpc::ClientContext* context, const std::string& username, const std::string& password) {
    const std::string formatted_credentials = string_format("%s:%s", username.c_str(), password.c_str());
    context->AddMetadata(AUTH_HEADER, BASIC_PREFIX + base64_encode(formatted_credentials));
  }

  std::string base64_encode(const std::string& input) {
     static const std::string base64_chars =
       "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
     auto get_encoded_length = [] (const std::string& in) {
       return 4 * ((in.size() + 2) / 3);
     };
     auto get_overwrite_count = [] (const std::string& in) {
       const std::string::size_type remainder = in.length() % 3;
       return (remainder > 0) ? (3 - (remainder % 3)) : 0;
     };

     // Generate string with required length for encoding.
     std::string encoded;
     encoded.reserve(get_encoded_length(input));

     // Loop through input writing base64 characters to string.
     for (int i = 0; i < input.length();) {
         uint32_t octet_1 = i < input.length() ? (unsigned char)input[i++] : 0;
         uint32_t octet_2 = i < input.length() ? (unsigned char)input[i++] : 0;
         uint32_t octet_3 = i < input.length() ? (unsigned char)input[i++] : 0;
         uint32_t octriple = (octet_1 << 0x10) + (octet_2 << 0x08) + octet_3;
         for (int j = 3; j >= 0; j--) {
             encoded.push_back(base64_chars[(octriple >> j * 6) & 0x3F]);
         }
     }

     // Round up to nearest multiple of 3 and replace characters at end based on rounding.
     int overwrite_count = get_overwrite_count(input);
     encoded.replace(encoded.length() - overwrite_count, 
                     encoded.length(), 
                     overwrite_count, '=');
     return encoded;
  }
}  // namespace flight
}  // namespace arrow