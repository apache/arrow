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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "arrow/util/base64.h"
#include "arrow/util/uri.h"

#include "parquet/exception.h"
#include "parquet/string_util.h"
#include "parquet/vault_client.h"

namespace parquet {
namespace encryption {

using arrow::internal::Uri;
using boost::asio::ip::tcp;

static constexpr char JSON_MEDIA_TYPE[] = "application/json; charset=utf-8";
static constexpr char DEFAULT_TRANSIT_ENGINE[] = "/v1/transit/";
static constexpr char TRANSIT_WRAP_ENDPOINT[] = "encrypt/";
static constexpr char TRANSIT_UNWRAP_ENDPOINT[] = "decrypt/";
static constexpr char TOKEN_HEADER[] = "X-Vault-Token";

bool StringEndWiths(const std::string s, char c) { return s[s.size() - 1] == c; }

void VaultClient::InitializeInternal() {
  if (is_default_token_) {
    throw new ParquetException("Vault token not provided");
  }

  std::string& kms_instance_url = kms_connection_config_.kms_instance_url;
  std::string& kms_instance_id = kms_connection_config_.kms_instance_id;

  if (kms_instance_url == KmsClient::KMS_INSTANCE_URL_DEFAULT) {
    throw new ParquetException("Vault URL not provided");
  }
  Uri uri;
  arrow::Status status = uri.Parse(kms_instance_url);
  if (!status.ok()) {
    throw new ParquetException("Invalid Vault URL");
  }

  host_ = uri.host();
  port_ = uri.port();

  std::string transit_engine = DEFAULT_TRANSIT_ENGINE;
  if (kms_instance_id != KmsClient::KMS_INSTANCE_ID_DEFAULT) {
    transit_engine = "/v1/" + kms_instance_id;
    if (!StringEndWiths(transit_engine, '/')) {
      transit_engine += "/";
    }
  }

  end_point_prefix_ = transit_engine;
}

std::string VaultClient::WrapKeyInServer(const std::string& key_bytes,
                                         const std::string& master_key_identifier) {
  std::string data_key_str = arrow::util::base64_encode(
      reinterpret_cast<const uint8_t*>(key_bytes.c_str()), key_bytes.size());

  std::map<std::string, std::string> write_key_map;
  write_key_map.insert({"plaintext", data_key_str});

  std::string response =
      GetContentFromTransitEngine(end_point_prefix_ + TRANSIT_WRAP_ENDPOINT,
                                  BuildPayload(write_key_map), master_key_identifier);

  std::string ciphertext = ParseReturn(response, "ciphertext");
  return ciphertext;
}

std::string VaultClient::UnwrapKeyInServer(const std::string& wrapped_key,
                                           const std::string& master_key_identifier) {
  std::map<std::string, std::string> write_key_map;
  write_key_map.insert({"ciphertext", wrapped_key});

  std::string response =
      GetContentFromTransitEngine(end_point_prefix_ + TRANSIT_UNWRAP_ENDPOINT,
                                  BuildPayload(write_key_map), master_key_identifier);
  std::string plaintext = ParseReturn(response, "plaintext");
  std::string key = arrow::util::base64_decode(plaintext);
  return key;
}

std::string VaultClient::BuildPayload(
    const std::map<std::string, std::string>& param_map) {
  rapidjson::Document d;
  auto& allocator = d.GetAllocator();
  rapidjson::Value json_map(rapidjson::kObjectType);

  rapidjson::Value key(rapidjson::kStringType);
  rapidjson::Value value(rapidjson::kStringType);
  for (auto it = param_map.begin(); it != param_map.end(); it++) {
    key.SetString(it->first.c_str(), allocator);
    value.SetString(it->second.c_str(), allocator);
    json_map.AddMember(key, value, allocator);
  }

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  json_map.Accept(writer);

  return buffer.GetString();
}

std::string VaultClient::GetContentFromTransitEngine(
    const std::string& sub_path, const std::string& json_payload,
    const std::string& master_key_identifier) {
  boost::asio::streambuf request;
  std::ostream request_stream(&request);

  request_stream << "POST " << sub_path << master_key_identifier << " HTTP/1.1\r\n";
  request_stream << "Host: " << host_ << "\r\n";
  request_stream << "Accept: */*\r\n";
  request_stream << TOKEN_HEADER << ":" << kms_connection_config_.key_access_token()
                 << "\r\n";
  request_stream << "Content-Length: " << json_payload.length() << "\r\n";
  request_stream << "Content-Type: " << JSON_MEDIA_TYPE << " \r\n";
  request_stream << "Connection: close\r\n";
  request_stream << "\r\n";
  request_stream << json_payload;

  return ExecuteAndGetResponse(sub_path, request);
}

std::string VaultClient::ExecuteAndGetResponse(const std::string& end_point,
                                               boost::asio::streambuf& request) {
  boost::asio::io_service io_service;
  tcp::socket socket(io_service);
  socket.connect(tcp::endpoint(boost::asio::ip::address::from_string(host_), port_));

  boost::system::error_code error;
  boost::asio::write(socket, request, error);
  if (error) {
    throw ParquetException("send failed: " + error.message());
  }

  boost::asio::streambuf response;

  // read response status
  boost::asio::read_until(socket, response, "\r\n", error);

  if (error && error != boost::asio::error::eof) {
    throw ParquetException("receive failed: " + error.message());
  } else {
    std::istream response_stream(&response);

    std::string http_version;
    response_stream >> http_version;
    if (!response_stream || http_version.substr(0, 5) != "HTTP/") {
      throw ParquetException("Invalid response");
    }

    unsigned int status_code;
    response_stream >> status_code;
    std::string status_message;
    std::getline(response_stream, status_message);
    if (status_code != 200) {
      std::stringstream ss;
      ss << "Response returned with status code: " << status_code << " "
         << status_message;
      throw ParquetException(ss.str());
    }

    // Read the response headers, which are terminated by a blank line.
    boost::asio::read_until(socket, response, "\r\n\r\n", error);
    std::string header_part;
    do {
      std::getline(response_stream, header_part);
    } while (header_part != "\r");

    // read response data
    boost::asio::read(socket, response, boost::asio::transfer_at_least(1), error);

    std::string data;
    response_stream >> data;
    return data;
  }
}

std::string VaultClient::ParseReturn(const std::string& response,
                                     const std::string& search_key) {
  rapidjson::Document document;
  document.Parse(response.c_str());

  if (document.HasParseError() || !document.IsObject()) {
    throw ParquetException("Failed to parse vault response. " + response);
  }

  if (!document.HasMember("data")) {
    throw ParquetException("Failed to parse vault response. " + search_key +
                           " not found. " + response);
  }

  auto data = document["data"].GetObject();

  std::string matching_value = data[search_key.c_str()].GetString();
  if (matching_value.empty()) {
    throw ParquetException("Failed to match vault response. " + search_key +
                           " not found. " + response);
  }

  return matching_value;
}

std::string VaultClient::GetMasterKeyFromServer(
    const std::string& master_key_identifier) {
  // Vault supports in-server wrapping and unwrapping. No need to fetch master keys.
  throw new ParquetException(
      "Use server wrap/unwrap, instead of fetching master keys (local wrap)");
}

}  // namespace encryption
}  // namespace parquet
