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

#include "arrow/filesystem/s3fs.h"

#include <algorithm>
#include <atomic>
#include <mutex>
#include <sstream>
#include <utility>

#ifdef _WIN32
// Undefine preprocessor macros that interfere with AWS function / method names
#ifdef GetMessage
#undef GetMessage
#endif
#ifdef GetObject
#undef GetObject
#endif
#endif

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListBucketsResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "arrow/buffer.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/s3_internal.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace fs {

using ::Aws::Client::AWSError;
using ::Aws::S3::S3Errors;
namespace S3Model = Aws::S3::Model;

using ::arrow::fs::internal::ConnectRetryStrategy;
using ::arrow::fs::internal::ErrorToStatus;
using ::arrow::fs::internal::FromAwsString;
using ::arrow::fs::internal::IsAlreadyExists;
using ::arrow::fs::internal::IsNotFound;
using ::arrow::fs::internal::OutcomeToStatus;
using ::arrow::fs::internal::ToAwsString;
using ::arrow::fs::internal::ToTimePoint;
using ::arrow::fs::internal::ToURLEncodedAwsString;

const char* kS3DefaultRegion = "us-east-1";

static const char kSep = '/';

static std::mutex aws_init_lock;
static Aws::SDKOptions aws_options;
static std::atomic<bool> aws_initialized(false);

Status InitializeS3(const S3GlobalOptions& options) {
  std::lock_guard<std::mutex> lock(aws_init_lock);
  Aws::Utils::Logging::LogLevel aws_log_level;

#define LOG_LEVEL_CASE(level_name)                             \
  case S3LogLevel::level_name:                                 \
    aws_log_level = Aws::Utils::Logging::LogLevel::level_name; \
    break;

  switch (options.log_level) {
    LOG_LEVEL_CASE(Fatal)
    LOG_LEVEL_CASE(Error)
    LOG_LEVEL_CASE(Warn)
    LOG_LEVEL_CASE(Info)
    LOG_LEVEL_CASE(Debug)
    LOG_LEVEL_CASE(Trace)
    default:
      aws_log_level = Aws::Utils::Logging::LogLevel::Off;
  }

#undef LOG_LEVEL_CASE

  aws_options.loggingOptions.logLevel = aws_log_level;
  // By default the AWS SDK logs to files, log to console instead
  aws_options.loggingOptions.logger_create_fn = [] {
    return std::make_shared<Aws::Utils::Logging::ConsoleLogSystem>(
        aws_options.loggingOptions.logLevel);
  };
  Aws::InitAPI(aws_options);
  aws_initialized.store(true);
  return Status::OK();
}

Status FinalizeS3() {
  std::lock_guard<std::mutex> lock(aws_init_lock);
  Aws::ShutdownAPI(aws_options);
  aws_initialized.store(false);
  return Status::OK();
}

namespace {

Status CheckS3Initialized() {
  if (!aws_initialized.load()) {
    return Status::Invalid(
        "S3 subsystem not initialized; please call InitializeS3() "
        "before carrying out any S3-related operation");
  }
  return Status::OK();
}

// XXX Sanitize paths by removing leading slash?

struct S3Path {
  std::string full_path;
  std::string bucket;
  std::string key;
  std::vector<std::string> key_parts;

  static Status FromString(const std::string& s, S3Path* out) {
    auto first_sep = s.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
    }
    if (first_sep == std::string::npos) {
      *out = {s, s, "", {}};
      return Status::OK();
    }
    out->full_path = s;
    out->bucket = s.substr(0, first_sep);
    out->key = s.substr(first_sep + 1);
    out->key_parts = internal::SplitAbstractPath(out->key);
    return internal::ValidateAbstractPathParts(out->key_parts);
  }

  Aws::String ToURLEncodedAwsString() const {
    // URL-encode individual parts, not the '/' separator
    Aws::String res;
    res += internal::ToURLEncodedAwsString(bucket);
    for (const auto& part : key_parts) {
      res += kSep;
      res += internal::ToURLEncodedAwsString(part);
    }
    return res;
  }

  S3Path parent() const {
    DCHECK(!key_parts.empty());
    auto parent = S3Path{"", bucket, "", key_parts};
    parent.key_parts.pop_back();
    parent.key = internal::JoinAbstractPath(parent.key_parts);
    parent.full_path = parent.bucket + kSep + parent.key;
    return parent;
  }

  bool has_parent() const { return !key.empty(); }

  bool empty() const { return bucket.empty() && key.empty(); }

  bool operator==(const S3Path& other) const {
    return bucket == other.bucket && key == other.key;
  }
};

// XXX return in OutcomeToStatus instead?
Status PathNotFound(const S3Path& path) {
  return Status::IOError("Path does not exist '", path.full_path, "'");
}

Status PathNotFound(const std::string& bucket, const std::string& key) {
  return Status::IOError("Path does not exist '", bucket, kSep, key, "'");
}

Status NotAFile(const S3Path& path) {
  return Status::IOError("Not a regular file: '", path.full_path, "'");
}

Status ValidateFilePath(const S3Path& path) {
  if (path.bucket.empty() || path.key.empty()) {
    return NotAFile(path);
  }
  return Status::OK();
}

std::string FormatRange(int64_t start, int64_t length) {
  // Format a HTTP range header value
  std::stringstream ss;
  ss << "bytes=" << start << "-" << start + length - 1;
  return ss.str();
}

Status GetObjectRange(Aws::S3::S3Client* client, const S3Path& path, int64_t start,
                      int64_t length, S3Model::GetObjectResult* out) {
  S3Model::GetObjectRequest req;
  req.SetBucket(ToAwsString(path.bucket));
  req.SetKey(ToAwsString(path.key));
  req.SetRange(ToAwsString(FormatRange(start, length)));
  ARROW_AWS_ASSIGN_OR_RAISE(*out, client->GetObject(req));
  return Status::OK();
}

// A RandomAccessFile that reads from a S3 object
class ObjectInputFile : public io::RandomAccessFile {
 public:
  ObjectInputFile(Aws::S3::S3Client* client, const S3Path& path)
      : client_(client), path_(path) {}

  Status Init() {
    // Issue a HEAD Object to get the content-length and ensure any
    // errors (e.g. file not found) don't wait until the first Read() call.
    S3Model::HeadObjectRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));

    auto outcome = client_->HeadObject(req);
    if (!outcome.IsSuccess()) {
      if (IsNotFound(outcome.GetError())) {
        return PathNotFound(path_);
      } else {
        return ErrorToStatus(
            std::forward_as_tuple("When reading information for key '", path_.key,
                                  "' in bucket '", path_.bucket, "': "),
            outcome.GetError());
      }
    }
    content_length_ = outcome.GetResult().GetContentLength();
    DCHECK_GE(content_length_, 0);
    return Status::OK();
  }

  Status CheckClosed() const {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    return Status::OK();
  }

  Status CheckPosition(int64_t position, const char* action) const {
    if (position < 0) {
      return Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > content_length_) {
      return Status::IOError("Cannot ", action, " past end of file");
    }
    return Status::OK();
  }

  // RandomAccessFile APIs

  Status Close() override {
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Status Tell(int64_t* position) const override {
    RETURN_NOT_OK(CheckClosed());

    *position = pos_;
    return Status::OK();
  }

  Status GetSize(int64_t* size) override {
    RETURN_NOT_OK(CheckClosed());

    *size = content_length_;
    return Status::OK();
  }

  Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return Status::OK();
  }

  Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                void* out) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // Read the desired range of bytes
    S3Model::GetObjectResult result;
    RETURN_NOT_OK(GetObjectRange(client_, path_, position, nbytes, &result));

    auto& stream = result.GetBody();
    stream.read(reinterpret_cast<char*>(out), nbytes);
    // NOTE: the stream is a stringstream by default, there is no actual error
    // to check for.  However, stream.fail() may return true if EOF is reached.
    *bytes_read = stream.gcount();
    return Status::OK();
  }

  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    std::shared_ptr<ResizableBuffer> buf;
    int64_t bytes_read;
    RETURN_NOT_OK(AllocateResizableBuffer(nbytes, &buf));
    if (nbytes > 0) {
      RETURN_NOT_OK(ReadAt(position, nbytes, &bytes_read, buf->mutable_data()));
      DCHECK_LE(bytes_read, nbytes);
      RETURN_NOT_OK(buf->Resize(bytes_read));
    }
    *out = std::move(buf);
    return Status::OK();
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override {
    RETURN_NOT_OK(ReadAt(pos_, nbytes, bytes_read, out));
    pos_ += *bytes_read;
    return Status::OK();
  }

  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override {
    RETURN_NOT_OK(ReadAt(pos_, nbytes, out));
    pos_ += (*out)->size();
    return Status::OK();
  }

 protected:
  Aws::S3::S3Client* client_;
  S3Path path_;
  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = -1;
};

// A non-copying istream.
// See https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
// https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory

class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
 public:
  StringViewStream(const void* data, int64_t nbytes)
      : Aws::Utils::Stream::PreallocatedStreamBuf(
            reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
            static_cast<size_t>(nbytes)),
        std::iostream(this) {}
};

// Minimum size for each part of a multipart upload, except for the last part.
// AWS doc says "5 MB" but it's not clear whether those are MB or MiB,
// so I chose the safer value.
// (see https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html)
static constexpr int64_t kMinimumPartUpload = 5 * 1024 * 1024;

// An OutputStream that writes to a S3 object
class ObjectOutputStream : public io::OutputStream {
 public:
  ObjectOutputStream(Aws::S3::S3Client* client, const S3Path& path)
      : client_(client), path_(path) {}

  ~ObjectOutputStream() override {
    if (!closed_) {
      auto st = Abort();
      if (!st.ok()) {
        ARROW_LOG(ERROR) << "Could not abort multipart upload: " << st;
      }
    }
  }

  Status Init() {
    // Initiate the multi-part upload
    S3Model::CreateMultipartUploadRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));

    auto outcome = client_->CreateMultipartUpload(req);
    if (!outcome.IsSuccess()) {
      return ErrorToStatus(
          std::forward_as_tuple("When initiating multiple part upload for key '",
                                path_.key, "' in bucket '", path_.bucket, "': "),
          outcome.GetError());
    }
    upload_id_ = outcome.GetResult().GetUploadId();
    closed_ = false;
    return Status::OK();
  }

  Status Abort() {
    S3Model::AbortMultipartUploadRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));
    req.SetUploadId(upload_id_);

    auto outcome = client_->AbortMultipartUpload(req);
    if (!outcome.IsSuccess()) {
      return ErrorToStatus(
          std::forward_as_tuple("When aborting multiple part upload for key '", path_.key,
                                "' in bucket '", path_.bucket, "': "),
          outcome.GetError());
    }
    current_part_.reset();
    closed_ = true;
    return Status::OK();
  }

  // OutputStream interface

  Status Close() override {
    if (closed_) {
      return Status::OK();
    }

    if (current_part_) {
      // Upload last part
      RETURN_NOT_OK(CommitCurrentPart());
    }

    // S3 mandates at least one part, upload an empty one if necessary
    if (completed_upload_.GetParts().empty()) {
      RETURN_NOT_OK(UploadPart("", 0));
    }
    DCHECK(completed_upload_.PartsHasBeenSet());

    S3Model::CompleteMultipartUploadRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));
    req.SetUploadId(upload_id_);
    req.SetMultipartUpload(completed_upload_);

    auto outcome = client_->CompleteMultipartUpload(req);
    if (!outcome.IsSuccess()) {
      return ErrorToStatus(
          std::forward_as_tuple("When completing multiple part upload for key '",
                                path_.key, "' in bucket '", path_.bucket, "': "),
          outcome.GetError());
    }

    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Status Tell(int64_t* position) const override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    *position = pos_;
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }

    // With up to 10000 parts in an upload (S3 limit), a stream writing chunks
    // of exactly 5MB would be limited to 50GB total.  To avoid that, we bump
    // the upload threshold every 100 parts.  So the pattern is:
    // - part 1 to 99: 5MB threshold
    // - part 100 to 199: 10MB threshold
    // - part 200 to 299: 15MB threshold
    // ...
    // - part 9900 to 9999: 500MB threshold
    // So the total size limit is 2475000MB or ~2.4TB, while keeping manageable
    // chunk sizes and avoiding too much buffering in the common case of a small-ish
    // stream.  If the limit's not enough, we can revisit.
    if (part_number_ % 100 == 0) {
      part_upload_threshold_ += kMinimumPartUpload;
    }

    if (!current_part_ && nbytes >= part_upload_threshold_) {
      // No current part and data large enough, upload it directly without copying
      RETURN_NOT_OK(UploadPart(data, nbytes));
      pos_ += nbytes;
      return Status::OK();
    }
    // Can't upload data on its own, need to buffer it
    if (!current_part_) {
      RETURN_NOT_OK(io::BufferOutputStream::Create(
          part_upload_threshold_, default_memory_pool(), &current_part_));
      current_part_size_ = 0;
    }
    RETURN_NOT_OK(current_part_->Write(data, nbytes));
    pos_ += nbytes;
    current_part_size_ += nbytes;

    if (current_part_size_ >= part_upload_threshold_) {
      // Current part large enough, upload it
      RETURN_NOT_OK(CommitCurrentPart());
    }

    return Status::OK();
  }

  Status Flush() override {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    return Status::OK();
  }

  Status CommitCurrentPart() {
    std::shared_ptr<Buffer> buf;
    RETURN_NOT_OK(current_part_->Finish(&buf));
    current_part_.reset();
    current_part_size_ = 0;
    return UploadPart(buf->data(), buf->size());
  }

  Status UploadPart(const void* data, int64_t nbytes) {
    S3Model::UploadPartRequest req;
    req.SetBucket(ToAwsString(path_.bucket));
    req.SetKey(ToAwsString(path_.key));
    req.SetUploadId(upload_id_);
    req.SetPartNumber(part_number_);
    req.SetContentLength(nbytes);
    req.SetBody(std::make_shared<StringViewStream>(data, nbytes));

    auto outcome = client_->UploadPart(req);
    if (!outcome.IsSuccess()) {
      return ErrorToStatus(
          std::forward_as_tuple("When uploading part for key '", path_.key,
                                "' in bucket '", path_.bucket, "': "),
          outcome.GetError());
    }
    // Append ETag and part number for this uploaded part
    // (will be needed for upload completion in Close())
    S3Model::CompletedPart part;
    part.SetPartNumber(part_number_);
    part.SetETag(outcome.GetResult().GetETag());
    completed_upload_.AddParts(std::move(part));
    ++part_number_;
    return Status::OK();
  }

 protected:
  Aws::S3::S3Client* client_;
  S3Path path_;
  Aws::String upload_id_;
  S3Model::CompletedMultipartUpload completed_upload_;
  bool closed_ = true;
  int64_t pos_ = 0;
  int32_t part_number_ = 1;
  std::shared_ptr<io::BufferOutputStream> current_part_;
  int64_t current_part_size_ = 0;
  int64_t part_upload_threshold_ = kMinimumPartUpload;
};

// This function assumes st->path() is already set
Status FileObjectToStats(const S3Model::HeadObjectResult& obj, FileStats* st) {
  st->set_type(FileType::File);
  st->set_size(static_cast<int64_t>(obj.GetContentLength()));
  st->set_mtime(ToTimePoint(obj.GetLastModified()));
  return Status::OK();
}

Status FileObjectToStats(const S3Model::Object& obj, FileStats* st) {
  st->set_type(FileType::File);
  st->set_size(static_cast<int64_t>(obj.GetSize()));
  st->set_mtime(ToTimePoint(obj.GetLastModified()));
  return Status::OK();
}

}  // namespace

class S3FileSystem::Impl {
 public:
  S3Options options_;
  Aws::Client::ClientConfiguration client_config_;
  Aws::Auth::AWSCredentials credentials_;
  std::unique_ptr<Aws::S3::S3Client> client_;

  const int32_t kListObjectsMaxKeys = 1000;
  // At most 1000 keys per multiple-delete request
  const int32_t kMultipleDeleteMaxKeys = 1000;
  // Limit recursing depth, since a recursion bomb can be created
  const int32_t kMaxNestingDepth = 100;

  explicit Impl(S3Options options) : options_(std::move(options)) {}

  Status Init() {
    credentials_ = {ToAwsString(options_.access_key), ToAwsString(options_.secret_key)};
    client_config_.region = ToAwsString(options_.region);
    client_config_.endpointOverride = ToAwsString(options_.endpoint_override);
    if (options_.scheme == "http") {
      client_config_.scheme = Aws::Http::Scheme::HTTP;
    } else if (options_.scheme == "https") {
      client_config_.scheme = Aws::Http::Scheme::HTTPS;
    } else {
      return Status::Invalid("Invalid S3 connection scheme '", options_.scheme, "'");
    }
    client_config_.retryStrategy = std::make_shared<ConnectRetryStrategy>();
    bool use_virtual_addressing = options_.endpoint_override.empty();
    client_.reset(
        new Aws::S3::S3Client(credentials_, client_config_,
                              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                              use_virtual_addressing));
    return Status::OK();
  }

  // Create a bucket.  Successful if bucket already exists.
  Status CreateBucket(const std::string& bucket) {
    S3Model::CreateBucketConfiguration config;
    S3Model::CreateBucketRequest req;
    config.SetLocationConstraint(
        S3Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(
            ToAwsString(options_.region)));
    req.SetBucket(ToAwsString(bucket));
    req.SetCreateBucketConfiguration(config);

    auto outcome = client_->CreateBucket(req);
    if (!outcome.IsSuccess() && !IsAlreadyExists(outcome.GetError())) {
      return ErrorToStatus(std::forward_as_tuple("When creating bucket '", bucket, "': "),
                           outcome.GetError());
    }
    return Status::OK();
  }

  // Create an object with empty contents.  Successful if object already exists.
  Status CreateEmptyObject(const std::string& bucket, const std::string& key) {
    S3Model::PutObjectRequest req;
    req.SetBucket(ToAwsString(bucket));
    req.SetKey(ToAwsString(key));
    return OutcomeToStatus(
        std::forward_as_tuple("When creating key '", key, "' in bucket '", bucket, "': "),
        client_->PutObject(req));
  }

  Status CreateEmptyDir(const std::string& bucket, const std::string& key) {
    DCHECK(!key.empty());
    return CreateEmptyObject(bucket, key + kSep);
  }

  Status DeleteObject(const std::string& bucket, const std::string& key) {
    S3Model::DeleteObjectRequest req;
    req.SetBucket(ToAwsString(bucket));
    req.SetKey(ToAwsString(key));
    return OutcomeToStatus(
        std::forward_as_tuple("When delete key '", key, "' in bucket '", bucket, "': "),
        client_->DeleteObject(req));
  }

  Status CopyObject(const S3Path& src_path, const S3Path& dest_path) {
    S3Model::CopyObjectRequest req;
    req.SetBucket(ToAwsString(dest_path.bucket));
    req.SetKey(ToAwsString(dest_path.key));
    // Copy source "Must be URL-encoded" according to AWS SDK docs.
    req.SetCopySource(src_path.ToURLEncodedAwsString());
    return OutcomeToStatus(
        std::forward_as_tuple("When copying key '", src_path.key, "' in bucket '",
                              src_path.bucket, "' to key '", dest_path.key,
                              "' in bucket '", dest_path.bucket, "': "),
        client_->CopyObject(req));
  }

  // On Minio, an empty "directory" doesn't satisfy the same API requests as
  // a non-empty "directory".  This is a Minio-specific quirk, but we need
  // to handle it for unit testing.

  Status IsEmptyDirectory(const std::string& bucket, const std::string& key, bool* out) {
    S3Model::HeadObjectRequest req;
    req.SetBucket(ToAwsString(bucket));
    req.SetKey(ToAwsString(key) + kSep);

    auto outcome = client_->HeadObject(req);
    if (outcome.IsSuccess()) {
      *out = true;
      return Status::OK();
    }
    if (IsNotFound(outcome.GetError())) {
      *out = false;
      return Status::OK();
    }
    return ErrorToStatus(std::forward_as_tuple("When reading information for key '", key,
                                               "' in bucket '", bucket, "': "),
                         outcome.GetError());
  }

  Status IsEmptyDirectory(const S3Path& path, bool* out) {
    return IsEmptyDirectory(path.bucket, path.key, out);
  }

  Status IsNonEmptyDirectory(const S3Path& path, bool* out) {
    S3Model::ListObjectsV2Request req;
    req.SetBucket(ToAwsString(path.bucket));
    req.SetPrefix(ToAwsString(path.key) + kSep);
    req.SetDelimiter(Aws::String() + kSep);
    req.SetMaxKeys(1);
    auto outcome = client_->ListObjectsV2(req);
    if (outcome.IsSuccess()) {
      *out = outcome.GetResult().GetKeyCount() > 0;
      return Status::OK();
    }
    if (IsNotFound(outcome.GetError())) {
      *out = false;
      return Status::OK();
    }
    return ErrorToStatus(
        std::forward_as_tuple("When listing objects under key '", path.key,
                              "' in bucket '", path.bucket, "': "),
        outcome.GetError());
  }

  // List objects under a given prefix, issuing continuation requests if necessary
  template <typename ResultCallable, typename ErrorCallable>
  Status ListObjectsV2(const std::string& bucket, const std::string& prefix,
                       ResultCallable&& result_callable, ErrorCallable&& error_callable) {
    S3Model::ListObjectsV2Request req;
    req.SetBucket(ToAwsString(bucket));
    if (!prefix.empty()) {
      req.SetPrefix(ToAwsString(prefix) + kSep);
    }
    req.SetDelimiter(Aws::String() + kSep);
    req.SetMaxKeys(kListObjectsMaxKeys);

    while (true) {
      auto outcome = client_->ListObjectsV2(req);
      if (!outcome.IsSuccess()) {
        return error_callable(outcome.GetError());
      }
      const auto& result = outcome.GetResult();
      RETURN_NOT_OK(result_callable(result));
      // Was the result limited by max-keys? If so, use the continuation token
      // to fetch further results.
      if (!result.GetIsTruncated()) {
        break;
      }
      DCHECK(!result.GetNextContinuationToken().empty());
      req.SetContinuationToken(result.GetNextContinuationToken());
    }
    return Status::OK();
  }

  // Recursive workhorse for GetTargetStats(Selector...)
  Status Walk(const Selector& select, const std::string& bucket, const std::string& key,
              std::vector<FileStats>* out) {
    int32_t nesting_depth = 0;
    return Walk(select, bucket, key, nesting_depth, out);
  }

  Status Walk(const Selector& select, const std::string& bucket, const std::string& key,
              int32_t nesting_depth, std::vector<FileStats>* out) {
    if (nesting_depth >= kMaxNestingDepth) {
      return Status::IOError("S3 filesystem tree exceeds maximum nesting depth (",
                             kMaxNestingDepth, ")");
    }

    bool is_empty = true;
    std::vector<std::string> child_keys;

    auto handle_results = [&](const S3Model::ListObjectsV2Result& result) -> Status {
      // Walk "files"
      for (const auto& obj : result.GetContents()) {
        is_empty = false;
        FileStats st;
        const auto child_key = internal::RemoveTrailingSlash(FromAwsString(obj.GetKey()));
        if (child_key == util::string_view(key)) {
          // Amazon can return the "directory" key itself as part of the results, skip
          continue;
        }
        std::stringstream child_path;
        child_path << bucket << kSep << child_key;
        st.set_path(child_path.str());
        RETURN_NOT_OK(FileObjectToStats(obj, &st));
        out->push_back(std::move(st));
      }
      // Walk "directories"
      for (const auto& prefix : result.GetCommonPrefixes()) {
        is_empty = false;
        const auto child_key =
            internal::RemoveTrailingSlash(FromAwsString(prefix.GetPrefix()));
        std::stringstream ss;
        ss << bucket << kSep << child_key;
        FileStats st;
        st.set_path(ss.str());
        st.set_type(FileType::Directory);
        out->push_back(std::move(st));
        if (select.recursive) {
          child_keys.emplace_back(child_key);
        }
      }
      return Status::OK();
    };

    auto handle_error = [&](const AWSError<S3Errors>& error) -> Status {
      if (select.allow_non_existent && IsNotFound(error)) {
        return Status::OK();
      }
      return ErrorToStatus(std::forward_as_tuple("When listing objects under key '", key,
                                                 "' in bucket '", bucket, "': "),
                           error);
    };

    RETURN_NOT_OK(
        ListObjectsV2(bucket, key, std::move(handle_results), std::move(handle_error)));

    // Recurse
    for (const auto& child_key : child_keys) {
      RETURN_NOT_OK(Walk(select, bucket, child_key, nesting_depth + 1, out));
    }

    // If no contents were found, perhaps it's an empty "directory",
    // or perhaps it's a non-existent entry.  Check.
    if (is_empty && !select.allow_non_existent) {
      RETURN_NOT_OK(IsEmptyDirectory(bucket, key, &is_empty));
      if (!is_empty) {
        return PathNotFound(bucket, key);
      }
    }
    return Status::OK();
  }

  Status WalkForDeleteDir(const std::string& bucket, const std::string& key,
                          std::vector<std::string>* file_keys,
                          std::vector<std::string>* dir_keys) {
    int32_t nesting_depth = 0;
    return WalkForDeleteDir(bucket, key, nesting_depth, file_keys, dir_keys);
  }

  Status WalkForDeleteDir(const std::string& bucket, const std::string& key,
                          int32_t nesting_depth, std::vector<std::string>* file_keys,
                          std::vector<std::string>* dir_keys) {
    if (nesting_depth >= kMaxNestingDepth) {
      return Status::IOError("S3 filesystem tree exceeds maximum nesting depth (",
                             kMaxNestingDepth, ")");
    }

    std::vector<std::string> child_keys;

    auto handle_results = [&](const S3Model::ListObjectsV2Result& result) -> Status {
      // Walk "files"
      for (const auto& obj : result.GetContents()) {
        file_keys->emplace_back(FromAwsString(obj.GetKey()));
      }
      // Walk "directories"
      for (const auto& prefix : result.GetCommonPrefixes()) {
        auto child_key = FromAwsString(prefix.GetPrefix());
        dir_keys->emplace_back(child_key);
        child_keys.emplace_back(internal::RemoveTrailingSlash(child_key));
      }
      return Status::OK();
    };

    auto handle_error = [&](const AWSError<S3Errors>& error) -> Status {
      return ErrorToStatus(std::forward_as_tuple("When listing objects under key '", key,
                                                 "' in bucket '", bucket, "': "),
                           error);
    };

    RETURN_NOT_OK(
        ListObjectsV2(bucket, key, std::move(handle_results), std::move(handle_error)));

    // Recurse
    for (const auto& child_key : child_keys) {
      RETURN_NOT_OK(
          WalkForDeleteDir(bucket, child_key, nesting_depth + 1, file_keys, dir_keys));
    }
    return Status::OK();
  }

  // Delete multiple objects at once
  Status DeleteObjects(const std::string& bucket, const std::vector<std::string>& keys) {
    const auto chunk_size = static_cast<size_t>(kMultipleDeleteMaxKeys);
    for (size_t start = 0; start < keys.size(); start += chunk_size) {
      S3Model::DeleteObjectsRequest req;
      S3Model::Delete del;
      for (size_t i = start; i < std::min(keys.size(), chunk_size); ++i) {
        del.AddObjects(S3Model::ObjectIdentifier().WithKey(ToAwsString(keys[i])));
      }
      req.SetBucket(ToAwsString(bucket));
      req.SetDelete(std::move(del));
      auto outcome = client_->DeleteObjects(req);
      if (!outcome.IsSuccess()) {
        return ErrorToStatus(outcome.GetError());
      }
      // Also need to check per-key errors, even on successful outcome
      // See
      // https://docs.aws.amazon.com/fr_fr/AmazonS3/latest/API/multiobjectdeleteapi.html
      const auto& errors = outcome.GetResult().GetErrors();
      if (!errors.empty()) {
        std::stringstream ss;
        ss << "Got the following " << errors.size()
           << " errors when deleting objects in S3 bucket '" << bucket << "':\n";
        for (const auto& error : errors) {
          ss << "- key '" << error.GetKey() << "': " << error.GetMessage() << "\n";
        }
        return Status::IOError(ss.str());
      }
    }
    return Status::OK();
  }

  Status DeleteDirContents(const std::string& bucket, const std::string& key) {
    std::vector<std::string> file_keys;
    std::vector<std::string> dir_keys;
    RETURN_NOT_OK(WalkForDeleteDir(bucket, key, &file_keys, &dir_keys));
    if (file_keys.empty() && dir_keys.empty() && !key.empty()) {
      // No contents found, is it an empty directory?
      bool exists = false;
      RETURN_NOT_OK(IsEmptyDirectory(bucket, key, &exists));
      if (!exists) {
        return PathNotFound(bucket, key);
      }
    }
    // First delete all "files", then delete all child "directories"
    RETURN_NOT_OK(DeleteObjects(bucket, file_keys));
    // XXX This doesn't seem necessary on Minio
    return DeleteObjects(bucket, dir_keys);
  }

  Status EnsureDirectoryExists(const S3Path& path) {
    if (!path.key.empty()) {
      return CreateEmptyDir(path.bucket, path.key);
    }
    return Status::OK();
  }

  Status EnsureParentExists(const S3Path& path) {
    if (path.has_parent()) {
      return EnsureDirectoryExists(path.parent());
    }
    return Status::OK();
  }

  Status ListBuckets(std::vector<std::string>* out) {
    out->clear();
    auto outcome = client_->ListBuckets();
    if (!outcome.IsSuccess()) {
      return ErrorToStatus(std::forward_as_tuple("When listing buckets: "),
                           outcome.GetError());
    }
    for (const auto& bucket : outcome.GetResult().GetBuckets()) {
      out->emplace_back(FromAwsString(bucket.GetName()));
    }
    return Status::OK();
  }
};

S3FileSystem::S3FileSystem(const S3Options& options) : impl_(new Impl{options}) {}

S3FileSystem::~S3FileSystem() {}

Status S3FileSystem::Make(const S3Options& options, std::shared_ptr<S3FileSystem>* out) {
  RETURN_NOT_OK(CheckS3Initialized());

  std::shared_ptr<S3FileSystem> ptr(new S3FileSystem(options));
  RETURN_NOT_OK(ptr->impl_->Init());
  *out = std::move(ptr);
  return Status::OK();
}

Status S3FileSystem::GetTargetStats(const std::string& s, FileStats* out) {
  S3Path path;
  RETURN_NOT_OK(S3Path::FromString(s, &path));
  FileStats st;
  st.set_path(s);

  if (path.empty()) {
    // It's the root path ""
    st.set_type(FileType::Directory);
    *out = st;
    return Status::OK();
  } else if (path.key.empty()) {
    // It's a bucket
    S3Model::HeadBucketRequest req;
    req.SetBucket(ToAwsString(path.bucket));

    auto outcome = impl_->client_->HeadBucket(req);
    if (!outcome.IsSuccess()) {
      if (!IsNotFound(outcome.GetError())) {
        return ErrorToStatus(
            std::forward_as_tuple("When getting information for bucket '", path.bucket,
                                  "': "),
            outcome.GetError());
      }
      st.set_type(FileType::NonExistent);
      *out = st;
      return Status::OK();
    }
    // NOTE: S3 doesn't have a bucket modification time.  Only a creation
    // time is available, and you have to list all buckets to get it.
    st.set_type(FileType::Directory);
    *out = st;
    return Status::OK();
  } else {
    // It's an object
    S3Model::HeadObjectRequest req;
    req.SetBucket(ToAwsString(path.bucket));
    req.SetKey(ToAwsString(path.key));

    auto outcome = impl_->client_->HeadObject(req);
    if (outcome.IsSuccess()) {
      // "File" object found
      *out = st;
      return FileObjectToStats(outcome.GetResult(), out);
    }
    if (!IsNotFound(outcome.GetError())) {
      return ErrorToStatus(
          std::forward_as_tuple("When getting information for key '", path.key,
                                "' in bucket '", path.bucket, "': "),
          outcome.GetError());
    }
    // Not found => perhaps it's an empty "directory"
    bool is_dir = false;
    RETURN_NOT_OK(impl_->IsEmptyDirectory(path, &is_dir));
    if (is_dir) {
      st.set_type(FileType::Directory);
      *out = st;
      return Status::OK();
    }
    // Not found => perhaps it's a non-empty "directory"
    RETURN_NOT_OK(impl_->IsNonEmptyDirectory(path, &is_dir));
    if (is_dir) {
      st.set_type(FileType::Directory);
    } else {
      st.set_type(FileType::NonExistent);
    }
    *out = st;
    return Status::OK();
  }
}

Status S3FileSystem::GetTargetStats(const Selector& select, std::vector<FileStats>* out) {
  S3Path base_path;
  RETURN_NOT_OK(S3Path::FromString(select.base_dir, &base_path));
  out->clear();

  if (base_path.empty()) {
    // List all buckets
    std::vector<std::string> buckets;
    RETURN_NOT_OK(impl_->ListBuckets(&buckets));
    for (const auto& bucket : buckets) {
      FileStats st;
      st.set_path(bucket);
      st.set_type(FileType::Directory);
      out->push_back(std::move(st));
      if (select.recursive) {
        RETURN_NOT_OK(impl_->Walk(select, bucket, "", out));
      }
    }
    return Status::OK();
  }

  // Nominal case -> walk a single bucket
  return impl_->Walk(select, base_path.bucket, base_path.key, out);
}

Status S3FileSystem::CreateDir(const std::string& s, bool recursive) {
  S3Path path;
  RETURN_NOT_OK(S3Path::FromString(s, &path));

  if (path.key.empty()) {
    // Create bucket
    return impl_->CreateBucket(path.bucket);
  }

  // Create object
  if (recursive) {
    // Ensure bucket exists
    RETURN_NOT_OK(impl_->CreateBucket(path.bucket));
    // Ensure that all parents exist, then the directory itself
    std::string parent_key;
    for (const auto& part : path.key_parts) {
      parent_key += part;
      parent_key += kSep;
      RETURN_NOT_OK(impl_->CreateEmptyObject(path.bucket, parent_key));
    }
    return Status::OK();
  } else {
    // Check parent dir exists
    if (path.has_parent()) {
      S3Path parent_path = path.parent();
      bool exists;
      RETURN_NOT_OK(impl_->IsNonEmptyDirectory(parent_path, &exists));
      if (!exists) {
        RETURN_NOT_OK(impl_->IsEmptyDirectory(parent_path, &exists));
      }
      if (!exists) {
        return Status::IOError("Cannot create directory '", path.full_path,
                               "': parent directory does not exist");
      }
    }

    // XXX Should we check that no non-directory entry exists?
    // Minio does it for us, not sure about other S3 implementations.
    return impl_->CreateEmptyDir(path.bucket, path.key);
  }
}

Status S3FileSystem::DeleteDir(const std::string& s) {
  S3Path path;
  RETURN_NOT_OK(S3Path::FromString(s, &path));

  if (path.empty()) {
    return Status::NotImplemented("Cannot delete all S3 buckets");
  }
  RETURN_NOT_OK(impl_->DeleteDirContents(path.bucket, path.key));
  if (path.key.empty()) {
    // Delete bucket
    S3Model::DeleteBucketRequest req;
    req.SetBucket(ToAwsString(path.bucket));
    return OutcomeToStatus(
        std::forward_as_tuple("When deleting bucket '", path.bucket, "': "),
        impl_->client_->DeleteBucket(req));
  } else {
    // Delete "directory"
    RETURN_NOT_OK(impl_->DeleteObject(path.bucket, path.key + kSep));
    // Parent may be implicitly deleted if it became empty, recreate it
    return impl_->EnsureParentExists(path);
  }
}

Status S3FileSystem::DeleteDirContents(const std::string& s) {
  S3Path path;
  RETURN_NOT_OK(S3Path::FromString(s, &path));

  if (path.empty()) {
    return Status::NotImplemented("Cannot delete all S3 buckets");
  }
  RETURN_NOT_OK(impl_->DeleteDirContents(path.bucket, path.key));
  // Directory may be implicitly deleted, recreate it
  return impl_->EnsureDirectoryExists(path);
}

Status S3FileSystem::DeleteFile(const std::string& s) {
  S3Path path;
  RETURN_NOT_OK(S3Path::FromString(s, &path));
  RETURN_NOT_OK(ValidateFilePath(path));

  // Check the object exists
  S3Model::HeadObjectRequest req;
  req.SetBucket(ToAwsString(path.bucket));
  req.SetKey(ToAwsString(path.key));

  auto outcome = impl_->client_->HeadObject(req);
  if (!outcome.IsSuccess()) {
    if (IsNotFound(outcome.GetError())) {
      return PathNotFound(path);
    } else {
      return ErrorToStatus(
          std::forward_as_tuple("When getting information for key '", path.key,
                                "' in bucket '", path.bucket, "': "),
          outcome.GetError());
    }
  }
  // Object found, delete it
  RETURN_NOT_OK(impl_->DeleteObject(path.bucket, path.key));
  // Parent may be implicitly deleted if it became empty, recreate it
  return impl_->EnsureParentExists(path);
}

Status S3FileSystem::Move(const std::string& src, const std::string& dest) {
  // XXX We don't implement moving directories as it would be too expensive:
  // one must copy all directory contents one by one (including object data),
  // then delete the original contents.

  S3Path src_path, dest_path;
  RETURN_NOT_OK(S3Path::FromString(src, &src_path));
  RETURN_NOT_OK(ValidateFilePath(src_path));
  RETURN_NOT_OK(S3Path::FromString(dest, &dest_path));
  RETURN_NOT_OK(ValidateFilePath(dest_path));

  if (src_path == dest_path) {
    return Status::OK();
  }
  RETURN_NOT_OK(impl_->CopyObject(src_path, dest_path));
  RETURN_NOT_OK(impl_->DeleteObject(src_path.bucket, src_path.key));
  // Source parent may be implicitly deleted if it became empty, recreate it
  return impl_->EnsureParentExists(src_path);
}

Status S3FileSystem::CopyFile(const std::string& src, const std::string& dest) {
  S3Path src_path, dest_path;
  RETURN_NOT_OK(S3Path::FromString(src, &src_path));
  RETURN_NOT_OK(ValidateFilePath(src_path));
  RETURN_NOT_OK(S3Path::FromString(dest, &dest_path));
  RETURN_NOT_OK(ValidateFilePath(dest_path));

  if (src_path == dest_path) {
    return Status::OK();
  }
  return impl_->CopyObject(src_path, dest_path);
}

Status S3FileSystem::OpenInputStream(const std::string& s,
                                     std::shared_ptr<io::InputStream>* out) {
  S3Path path;
  RETURN_NOT_OK(S3Path::FromString(s, &path));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr = std::make_shared<ObjectInputFile>(impl_->client_.get(), path);
  RETURN_NOT_OK(ptr->Init());
  *out = std::move(ptr);
  return Status::OK();
}

Status S3FileSystem::OpenInputFile(const std::string& s,
                                   std::shared_ptr<io::RandomAccessFile>* out) {
  S3Path path;
  RETURN_NOT_OK(S3Path::FromString(s, &path));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr = std::make_shared<ObjectInputFile>(impl_->client_.get(), path);
  RETURN_NOT_OK(ptr->Init());
  *out = std::move(ptr);
  return Status::OK();
}

Status S3FileSystem::OpenOutputStream(const std::string& s,
                                      std::shared_ptr<io::OutputStream>* out) {
  S3Path path;
  RETURN_NOT_OK(S3Path::FromString(s, &path));
  RETURN_NOT_OK(ValidateFilePath(path));

  auto ptr = std::make_shared<ObjectOutputStream>(impl_->client_.get(), path);
  RETURN_NOT_OK(ptr->Init());
  *out = std::move(ptr);
  return Status::OK();
}

Status S3FileSystem::OpenAppendStream(const std::string& path,
                                      std::shared_ptr<io::OutputStream>* out) {
  // XXX Investigate UploadPartCopy? Does it work with source == destination?
  // https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
  // (but would need to fall back to GET if the current data is < 5 MB)
  return Status::NotImplemented("It is not possible to append efficiently to S3 objects");
}

}  // namespace fs
}  // namespace arrow
