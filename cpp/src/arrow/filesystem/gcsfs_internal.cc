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

#include "arrow/filesystem/gcsfs_internal.h"

#include <absl/time/time.h>  // NOLINT
#include <google/cloud/storage/client.h>

#include <cerrno>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "arrow/filesystem/path_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/string.h"

namespace arrow {

using internal::ToChars;

namespace fs {
namespace internal {

using GcsCode = google::cloud::StatusCode;

int ErrnoFromStatus(const google::cloud::Status& s) {
  switch (s.code()) {
    case GcsCode::kAlreadyExists:
      return EEXIST;
    case GcsCode::kInvalidArgument:
      return EINVAL;
    case GcsCode::kNotFound:
      return ENOENT;
    case GcsCode::kPermissionDenied:
    case GcsCode::kUnauthenticated:
      return EACCES;
    default:
      return 0;
  }
}

Status ToArrowStatus(const google::cloud::Status& s) {
  std::ostringstream os;
  os << "google::cloud::Status(" << s << ")";
  Status st;
  switch (s.code()) {
    case GcsCode::kCancelled:
      st = Status::Cancelled(os.str());
      break;
    case GcsCode::kUnknown:
      st = Status::UnknownError(os.str());
      break;
    case GcsCode::kInvalidArgument:
      st = Status::Invalid(os.str());
      break;
    case GcsCode::kDeadlineExceeded:
    case GcsCode::kNotFound:
      st = Status::IOError(os.str());
      break;
    case GcsCode::kAlreadyExists:
      st = Status::AlreadyExists(os.str());
      break;
    case GcsCode::kPermissionDenied:
    case GcsCode::kUnauthenticated:
      st = Status::IOError(os.str());
      break;
    case GcsCode::kResourceExhausted:
      st = Status::CapacityError(os.str());
      break;
    case GcsCode::kFailedPrecondition:
    case GcsCode::kAborted:
      st = Status::IOError(os.str());
      break;
    case GcsCode::kOutOfRange:
      st = Status::Invalid(os.str());
      break;
    case GcsCode::kUnimplemented:
      st = Status::NotImplemented(os.str());
      break;
    case GcsCode::kInternal:
    case GcsCode::kUnavailable:
    case GcsCode::kDataLoss:
      st = Status::IOError(os.str());
      break;
    default:
      return Status::OK();
  }
  int errnum = ErrnoFromStatus(s);
  if (errnum) {
    st = st.WithDetail(::arrow::internal::StatusDetailFromErrno(errnum));
  }
  return st;
}

namespace gcs = ::google::cloud::storage;

Result<gcs::EncryptionKey> ToEncryptionKey(
    const std::shared_ptr<const KeyValueMetadata>& metadata) {
  if (!metadata) {
    return gcs::EncryptionKey{};
  }

  const auto& keys = metadata->keys();
  const auto& values = metadata->values();

  for (std::size_t i = 0; i < keys.size(); ++i) {
    if (keys[i] == "encryptionKeyBase64") {
      return gcs::EncryptionKey::FromBase64Key(values[i]);
    }
  }
  return gcs::EncryptionKey{};
}

Result<gcs::KmsKeyName> ToKmsKeyName(
    const std::shared_ptr<const KeyValueMetadata>& metadata) {
  if (!metadata) {
    return gcs::KmsKeyName{};
  }

  const auto& keys = metadata->keys();
  const auto& values = metadata->values();

  for (std::size_t i = 0; i < keys.size(); ++i) {
    if (keys[i] == "kmsKeyName") {
      return gcs::KmsKeyName(values[i]);
    }
  }
  return gcs::KmsKeyName{};
}

Result<gcs::PredefinedAcl> ToPredefinedAcl(
    const std::shared_ptr<const KeyValueMetadata>& metadata) {
  if (!metadata) {
    return gcs::PredefinedAcl{};
  }

  const auto& keys = metadata->keys();
  const auto& values = metadata->values();

  for (std::size_t i = 0; i < keys.size(); ++i) {
    if (keys[i] == "predefinedAcl") {
      return gcs::PredefinedAcl(values[i]);
    }
  }
  return gcs::PredefinedAcl{};
}

Result<gcs::WithObjectMetadata> ToObjectMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata) {
  if (!metadata) {
    return gcs::WithObjectMetadata{};
  }

  static auto const setters = [] {
    using setter = std::function<Status(gcs::ObjectMetadata&, const std::string&)>;
    return std::unordered_map<std::string, setter>{
        {"Cache-Control",
         [](gcs::ObjectMetadata& m, const std::string& v) {
           m.set_cache_control(v);
           return Status::OK();
         }},
        {"Content-Disposition",
         [](gcs::ObjectMetadata& m, const std::string& v) {
           m.set_content_disposition(v);
           return Status::OK();
         }},
        {"Content-Encoding",
         [](gcs::ObjectMetadata& m, const std::string& v) {
           m.set_content_encoding(v);
           return Status::OK();
         }},
        {"Content-Language",
         [](gcs::ObjectMetadata& m, const std::string& v) {
           m.set_content_language(v);
           return Status::OK();
         }},
        {"Content-Type",
         [](gcs::ObjectMetadata& m, const std::string& v) {
           m.set_content_type(v);
           return Status::OK();
         }},
        {"customTime",
         [](gcs::ObjectMetadata& m, const std::string& v) {
           std::string err;
           absl::Time t;
           if (!absl::ParseTime(absl::RFC3339_full, v, &t, &err)) {
             return Status::Invalid("Error parsing RFC-3339 timestamp: '", v, "': ", err);
           }
           m.set_custom_time(absl::ToChronoTime(t));
           return Status::OK();
         }},
        {"storageClass",
         [](gcs::ObjectMetadata& m, const std::string& v) {
           m.set_storage_class(v);
           return Status::OK();
         }},
        {"predefinedAcl",
         [](gcs::ObjectMetadata&, const std::string&) { return Status::OK(); }},
        {"encryptionKeyBase64",
         [](gcs::ObjectMetadata&, const std::string&) { return Status::OK(); }},
        {"kmsKeyName",
         [](gcs::ObjectMetadata&, const std::string&) { return Status::OK(); }},
    };
  }();

  const auto& keys = metadata->keys();
  const auto& values = metadata->values();

  gcs::ObjectMetadata object_metadata;
  for (std::size_t i = 0; i < keys.size(); ++i) {
    auto it = setters.find(keys[i]);
    if (it != setters.end()) {
      auto status = it->second(object_metadata, values[i]);
      if (!status.ok()) return status;
    } else {
      object_metadata.upsert_metadata(keys[i], values[i]);
    }
  }
  return gcs::WithObjectMetadata(std::move(object_metadata));
}

Result<std::shared_ptr<const KeyValueMetadata>> FromObjectMetadata(
    gcs::ObjectMetadata const& m) {
  auto format_time = [](std::chrono::system_clock::time_point tp) {
    return absl::FormatTime(absl::RFC3339_full, absl::FromChrono(tp),
                            absl::UTCTimeZone());
  };
  auto result = std::make_shared<KeyValueMetadata>();
  // The fields are in the same order as defined in:
  //  https://cloud.google.com/storage/docs/json_api/v1/objects
  // Where previous practice in Arrow suggested using a different field name (Content-Type
  // vs. contentType) we prefer the existing practice in Arrow.
  result->Append("id", m.id());
  result->Append("selfLink", m.self_link());
  result->Append("name", m.name());
  result->Append("bucket", m.bucket());
  result->Append("generation", ToChars(m.generation()));
  result->Append("Content-Type", m.content_type());
  result->Append("timeCreated", format_time(m.time_created()));
  result->Append("updated", format_time(m.updated()));
  if (m.has_custom_time()) {
    result->Append("customTime", format_time(m.custom_time()));
  }
  if (m.time_deleted() != std::chrono::system_clock::time_point()) {
    result->Append("timeDeleted", format_time(m.time_deleted()));
  }
  result->Append("temporaryHold", m.temporary_hold() ? "true" : "false");
  result->Append("eventBasedHold", m.event_based_hold() ? "true" : "false");
  if (m.retention_expiration_time() != std::chrono::system_clock::time_point()) {
    result->Append("retentionExpirationTime", format_time(m.retention_expiration_time()));
  }
  result->Append("storageClass", m.storage_class());
  if (m.time_storage_class_updated() != std::chrono::system_clock::time_point()) {
    result->Append("timeStorageClassUpdated",
                   format_time(m.time_storage_class_updated()));
  }
  result->Append("size", ToChars(m.size()));
  result->Append("md5Hash", m.md5_hash());
  result->Append("mediaLink", m.media_link());
  result->Append("Content-Encoding", m.content_encoding());
  result->Append("Content-Disposition", m.content_disposition());
  result->Append("Content-Language", m.content_language());
  result->Append("Cache-Control", m.cache_control());
  for (auto const& kv : m.metadata()) {
    result->Append("metadata." + kv.first, kv.second);
  }
  // Skip "acl" because it is overly complex
  if (m.has_owner()) {
    result->Append("owner.entity", m.owner().entity);
    result->Append("owner.entityId", m.owner().entity_id);
  }
  result->Append("crc32c", m.crc32c());
  result->Append("componentCount", ToChars(m.component_count()));
  result->Append("etag", m.etag());
  if (m.has_customer_encryption()) {
    result->Append("customerEncryption.encryptionAlgorithm",
                   m.customer_encryption().encryption_algorithm);
    result->Append("customerEncryption.keySha256", m.customer_encryption().key_sha256);
  }
  if (!m.kms_key_name().empty()) {
    result->Append("kmsKeyName", m.kms_key_name());
  }
  return result;
}

std::int64_t Depth(std::string_view path) {
  // The last slash is not counted towards depth because it represents a
  // directory.
  bool has_trailing_slash = !path.empty() && path.back() == '/';
  return std::count(path.begin(), path.end(), fs::internal::kSep) - has_trailing_slash;
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow
