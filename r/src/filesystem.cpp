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

#include "./arrow_types.h"
#include "./safe-call-into-r.h"

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/util/key_value_metadata.h>

namespace fs = ::arrow::fs;
namespace io = ::arrow::io;

namespace cpp11 {

const char* r6_class_name<fs::FileSystem>::get(
    const std::shared_ptr<fs::FileSystem>& file_system) {
  auto type_name = file_system->type_name();

  if (type_name == "local") {
    return "LocalFileSystem";
  } else if (type_name == "s3") {
    return "S3FileSystem";
  } else if (type_name == "gcs") {
    return "GcsFileSystem";
    // Uncomment these once R6 classes for these filesystems are added
    // } else if (type_name == "abfs") {
    //   return "AzureBlobFileSystem";
    // } else if (type_name == "hdfs") {
    //   return "HadoopFileSystem";
  } else if (type_name == "subtree") {
    return "SubTreeFileSystem";
  } else {
    return "FileSystem";
  }
}

}  // namespace cpp11

// [[arrow::export]]
fs::FileType fs___FileInfo__type(const std::shared_ptr<fs::FileInfo>& x) {
  return x->type();
}

// [[arrow::export]]
void fs___FileInfo__set_type(const std::shared_ptr<fs::FileInfo>& x, fs::FileType type) {
  x->set_type(type);
}

// [[arrow::export]]
std::string fs___FileInfo__path(const std::shared_ptr<fs::FileInfo>& x) {
  return x->path();
}

// [[arrow::export]]
void fs___FileInfo__set_path(const std::shared_ptr<fs::FileInfo>& x,
                             const std::string& path) {
  x->set_path(path);
}

// [[arrow::export]]
r_vec_size fs___FileInfo__size(const std::shared_ptr<fs::FileInfo>& x) {
  return r_vec_size(x->size());
}

// [[arrow::export]]
void fs___FileInfo__set_size(const std::shared_ptr<fs::FileInfo>& x, int64_t size) {
  x->set_size(size);
}

// [[arrow::export]]
std::string fs___FileInfo__base_name(const std::shared_ptr<fs::FileInfo>& x) {
  return x->base_name();
}

// [[arrow::export]]
std::string fs___FileInfo__extension(const std::shared_ptr<fs::FileInfo>& x) {
  return x->extension();
}

// [[arrow::export]]
SEXP fs___FileInfo__mtime(const std::shared_ptr<fs::FileInfo>& x) {
  SEXP res = PROTECT(Rf_allocVector(REALSXP, 1));
  // .mtime() gets us nanoseconds since epoch, POSIXct is seconds since epoch as a double
  REAL(res)[0] = static_cast<double>(x->mtime().time_since_epoch().count()) / 1000000000;
  Rf_classgets(res, arrow::r::data::classes_POSIXct);
  UNPROTECT(1);
  return res;
}

// [[arrow::export]]
void fs___FileInfo__set_mtime(const std::shared_ptr<fs::FileInfo>& x, SEXP time) {
  auto nanosecs =
      std::chrono::nanoseconds(static_cast<int64_t>(REAL(time)[0] * 1000000000));
  x->set_mtime(fs::TimePoint(nanosecs));
}

// Selector

// [[arrow::export]]
std::string fs___FileSelector__base_dir(
    const std::shared_ptr<fs::FileSelector>& selector) {
  return selector->base_dir;
}

// [[arrow::export]]
bool fs___FileSelector__allow_not_found(
    const std::shared_ptr<fs::FileSelector>& selector) {
  return selector->allow_not_found;
}

// [[arrow::export]]
bool fs___FileSelector__recursive(const std::shared_ptr<fs::FileSelector>& selector) {
  return selector->recursive;
}

// [[arrow::export]]
std::shared_ptr<fs::FileSelector> fs___FileSelector__create(const std::string& base_dir,
                                                            bool allow_not_found,
                                                            bool recursive) {
  auto selector = std::make_shared<fs::FileSelector>();
  selector->base_dir = base_dir;
  selector->allow_not_found = allow_not_found;
  selector->recursive = recursive;
  return selector;
}

// FileSystem

template <typename T>
std::vector<std::shared_ptr<T>> shared_ptr_vector(const std::vector<T>& vec) {
  std::vector<std::shared_ptr<fs::FileInfo>> res(vec.size());
  std::transform(vec.begin(), vec.end(), res.begin(),
                 [](const fs::FileInfo& x) { return std::make_shared<fs::FileInfo>(x); });
  return res;
}

// [[arrow::export]]
cpp11::list fs___FileSystem__GetTargetInfos_Paths(
    const std::shared_ptr<fs::FileSystem>& file_system,
    const std::vector<std::string>& paths) {
  auto results = ValueOrStop(file_system->GetFileInfo(paths));
  return arrow::r::to_r_list(shared_ptr_vector(results));
}

// [[arrow::export]]
cpp11::list fs___FileSystem__GetTargetInfos_FileSelector(
    const std::shared_ptr<fs::FileSystem>& file_system,
    const std::shared_ptr<fs::FileSelector>& selector) {
  auto results = ValueOrStop(file_system->GetFileInfo(*selector));

  return arrow::r::to_r_list(shared_ptr_vector(results));
}

// [[arrow::export]]
void fs___FileSystem__CreateDir(const std::shared_ptr<fs::FileSystem>& file_system,
                                const std::string& path, bool recursive) {
  StopIfNotOk(file_system->CreateDir(path, recursive));
}

// [[arrow::export]]
void fs___FileSystem__DeleteDir(const std::shared_ptr<fs::FileSystem>& file_system,
                                const std::string& path) {
  StopIfNotOk(file_system->DeleteDir(path));
}

// [[arrow::export]]
void fs___FileSystem__DeleteDirContents(
    const std::shared_ptr<fs::FileSystem>& file_system, const std::string& path) {
  StopIfNotOk(file_system->DeleteDirContents(path));
}

// [[arrow::export]]
void fs___FileSystem__DeleteFile(const std::shared_ptr<fs::FileSystem>& file_system,
                                 const std::string& path) {
  StopIfNotOk(file_system->DeleteFile(path));
}

// [[arrow::export]]
void fs___FileSystem__DeleteFiles(const std::shared_ptr<fs::FileSystem>& file_system,
                                  const std::vector<std::string>& paths) {
  StopIfNotOk(file_system->DeleteFiles(paths));
}

// [[arrow::export]]
void fs___FileSystem__Move(const std::shared_ptr<fs::FileSystem>& file_system,
                           const std::string& src, const std::string& dest) {
  StopIfNotOk(file_system->Move(src, dest));
}

// [[arrow::export]]
void fs___FileSystem__CopyFile(const std::shared_ptr<fs::FileSystem>& file_system,
                               const std::string& src, const std::string& dest) {
  StopIfNotOk(file_system->CopyFile(src, dest));
}

// [[arrow::export]]
std::shared_ptr<arrow::io::InputStream> fs___FileSystem__OpenInputStream(
    const std::shared_ptr<fs::FileSystem>& file_system, const std::string& path) {
  return ValueOrStop(file_system->OpenInputStream(path));
}

// [[arrow::export]]
std::shared_ptr<arrow::io::RandomAccessFile> fs___FileSystem__OpenInputFile(
    const std::shared_ptr<fs::FileSystem>& file_system, const std::string& path) {
  return ValueOrStop(file_system->OpenInputFile(path));
}

// [[arrow::export]]
std::shared_ptr<arrow::io::OutputStream> fs___FileSystem__OpenOutputStream(
    const std::shared_ptr<fs::FileSystem>& file_system, const std::string& path) {
  return ValueOrStop(file_system->OpenOutputStream(path));
}

// [[arrow::export]]
std::shared_ptr<arrow::io::OutputStream> fs___FileSystem__OpenAppendStream(
    const std::shared_ptr<fs::FileSystem>& file_system, const std::string& path) {
  return ValueOrStop(file_system->OpenAppendStream(path));
}

// [[arrow::export]]
std::string fs___FileSystem__type_name(
    const std::shared_ptr<fs::FileSystem>& file_system) {
  return file_system->type_name();
}

// [[arrow::export]]
std::shared_ptr<fs::LocalFileSystem> fs___LocalFileSystem__create() {
  // Affects OpenInputFile/OpenInputStream
  auto io_context = MainRThread::GetInstance().CancellableIOContext();
  return std::make_shared<fs::LocalFileSystem>(io_context);
}

// [[arrow::export]]
std::shared_ptr<fs::SubTreeFileSystem> fs___SubTreeFileSystem__create(
    const std::string& base_path, const std::shared_ptr<fs::FileSystem>& base_fs) {
  return std::make_shared<fs::SubTreeFileSystem>(base_path, base_fs);
}

// [[arrow::export]]
std::shared_ptr<fs::FileSystem> fs___SubTreeFileSystem__base_fs(
    const std::shared_ptr<fs::SubTreeFileSystem>& file_system) {
  return file_system->base_fs();
}

// [[arrow::export]]
std::string fs___SubTreeFileSystem__base_path(
    const std::shared_ptr<fs::SubTreeFileSystem>& file_system) {
  return file_system->base_path();
}

// [[arrow::export]]
cpp11::writable::list fs___FileSystemFromUri(const std::string& path) {
  using cpp11::literals::operator"" _nm;

  std::string out_path;
  return cpp11::writable::list(
      {"fs"_nm = cpp11::to_r6(ValueOrStop(fs::FileSystemFromUri(path, &out_path))),
       "path"_nm = out_path});
}

// [[arrow::export]]
void fs___CopyFiles(const std::shared_ptr<fs::FileSystem>& source_fs,
                    const std::shared_ptr<fs::FileSelector>& source_sel,
                    const std::shared_ptr<fs::FileSystem>& destination_fs,
                    const std::string& destination_base_dir,
                    int64_t chunk_size = 1024 * 1024, bool use_threads = true) {
  StopIfNotOk(fs::CopyFiles(source_fs, *source_sel, destination_fs, destination_base_dir,
                            io::default_io_context(), chunk_size, use_threads));
}

#if defined(ARROW_R_WITH_S3)

#include <arrow/filesystem/s3fs.h>

// [[s3::export]]
std::shared_ptr<fs::S3FileSystem> fs___S3FileSystem__create(
    bool anonymous = false, std::string access_key = "", std::string secret_key = "",
    std::string session_token = "", std::string role_arn = "",
    std::string session_name = "", std::string external_id = "", int load_frequency = 900,
    std::string region = "", std::string endpoint_override = "", std::string scheme = "",
    std::string proxy_options = "", bool background_writes = true,
    bool allow_bucket_creation = false, bool allow_bucket_deletion = false,
    double connect_timeout = -1, double request_timeout = -1) {
  // We need to ensure that S3 is initialized before we start messing with the
  // options
  StopIfNotOk(fs::EnsureS3Initialized());
  fs::S3Options s3_opts;
  // Handle auth (anonymous, keys, default)
  // (validation/internal coherence handled in R)
  if (anonymous) {
    s3_opts = fs::S3Options::Anonymous();
  } else if (access_key != "" && secret_key != "") {
    s3_opts = fs::S3Options::FromAccessKey(access_key, secret_key, session_token);
  } else if (role_arn != "") {
    s3_opts = fs::S3Options::FromAssumeRole(role_arn, session_name, external_id,
                                            load_frequency);
  } else {
    s3_opts = fs::S3Options::Defaults();
  }

  // Now handle the rest of the options
  /// AWS region to connect to (default determined by AWS SDK)
  if (region != "") {
    s3_opts.region = region;
  }
  /// If non-empty, override region with a connect string such as "localhost:9000"
  s3_opts.endpoint_override = endpoint_override;
  /// S3 connection transport, default "https"
  if (scheme != "") {
    s3_opts.scheme = scheme;
  }

  if (proxy_options != "") {
    auto s3_proxy_opts = fs::S3ProxyOptions::FromUri(proxy_options);
    s3_opts.proxy_options = ValueOrStop(s3_proxy_opts);
  }

  /// Whether OutputStream writes will be issued in the background, without blocking
  /// default true
  s3_opts.background_writes = background_writes;

  s3_opts.allow_bucket_creation = allow_bucket_creation;
  s3_opts.allow_bucket_deletion = allow_bucket_deletion;

  s3_opts.request_timeout = request_timeout;
  s3_opts.connect_timeout = connect_timeout;

  auto io_context = MainRThread::GetInstance().CancellableIOContext();
  return ValueOrStop(fs::S3FileSystem::Make(s3_opts, io_context));
}

// [[s3::export]]
std::string fs___S3FileSystem__region(const std::shared_ptr<fs::S3FileSystem>& fs) {
  return fs->region();
}

#endif

// [[arrow::export]]
void FinalizeS3() {
#if defined(ARROW_R_WITH_S3)
  StopIfNotOk(fs::FinalizeS3());
#endif
}

#if defined(ARROW_R_WITH_GCS)

#include <arrow/filesystem/gcsfs.h>

std::shared_ptr<arrow::KeyValueMetadata> strings_to_kvm(cpp11::strings metadata);

// [[gcs::export]]
std::shared_ptr<fs::GcsFileSystem> fs___GcsFileSystem__Make(bool anonymous,
                                                            cpp11::list options) {
  fs::GcsOptions gcs_opts;

  // Handle auth (anonymous, credentials, default)
  // (validation/internal coherence handled in R)
  if (anonymous) {
    gcs_opts = fs::GcsOptions::Anonymous();
  } else if (!Rf_isNull(options["access_token"])) {
    // Convert POSIXct timestamp seconds to nanoseconds
    std::chrono::nanoseconds ns_count(
        static_cast<int64_t>(cpp11::as_cpp<double>(options["expiration"])) * 1000000000);
    auto expiration_timepoint =
        fs::TimePoint(std::chrono::duration_cast<fs::TimePoint::duration>(ns_count));
    gcs_opts = fs::GcsOptions::FromAccessToken(
        cpp11::as_cpp<std::string>(options["access_token"]), expiration_timepoint);
    // TODO(ARROW-16885): implement FromImpersonatedServiceAccount
    // } else if (base_credentials != "") {
    //   // static GcsOptions FromImpersonatedServiceAccount(
    //   // const GcsCredentials& base_credentials, const std::string&
    //   target_service_account);
    //   // TODO: construct GcsCredentials
    //   gcs_opts = fs::GcsOptions::FromImpersonatedServiceAccount(base_credentials,
    //                                                             target_service_account);
  } else if (!Rf_isNull(options["json_credentials"])) {
    gcs_opts = fs::GcsOptions::FromServiceAccountCredentials(
        cpp11::as_cpp<std::string>(options["json_credentials"]));
  } else {
    gcs_opts = fs::GcsOptions::Defaults();
  }

  // Handle other attributes
  if (!Rf_isNull(options["endpoint_override"])) {
    gcs_opts.endpoint_override = cpp11::as_cpp<std::string>(options["endpoint_override"]);
  }

  if (!Rf_isNull(options["scheme"])) {
    gcs_opts.scheme = cpp11::as_cpp<std::string>(options["scheme"]);
  }

  // /// \brief Location to use for creating buckets.
  if (!Rf_isNull(options["default_bucket_location"])) {
    gcs_opts.default_bucket_location =
        cpp11::as_cpp<std::string>(options["default_bucket_location"]);
  }
  // /// \brief If set used to control total time allowed for retrying underlying
  // /// errors.
  // ///
  // /// The default policy is to retry for up to 15 minutes.
  if (!Rf_isNull(options["retry_limit_seconds"])) {
    gcs_opts.retry_limit_seconds = cpp11::as_cpp<double>(options["retry_limit_seconds"]);
  }

  // /// \brief Default metadata for OpenOutputStream.
  // ///
  // /// This will be ignored if non-empty metadata is passed to OpenOutputStream.
  if (!Rf_isNull(options["default_metadata"])) {
    gcs_opts.default_metadata = strings_to_kvm(options["default_metadata"]);
  }

  auto io_context = MainRThread::GetInstance().CancellableIOContext();
  // TODO(ARROW-16884): update when this returns Result
  return fs::GcsFileSystem::Make(gcs_opts, io_context);
}

// [[gcs::export]]
cpp11::list fs___GcsFileSystem__options(const std::shared_ptr<fs::GcsFileSystem>& fs) {
  using cpp11::literals::operator"" _nm;

  cpp11::writable::list out;

  fs::GcsOptions opts = fs->options();

  // GcsCredentials
  out.push_back({"anonymous"_nm = opts.credentials.anonymous()});

  if (opts.credentials.access_token() != "") {
    out.push_back({"access_token"_nm = opts.credentials.access_token()});
  }

  if (opts.credentials.expiration().time_since_epoch().count() != 0) {
    out.push_back({"expiration"_nm = cpp11::as_sexp<double>(
                       opts.credentials.expiration().time_since_epoch().count())});
  }

  if (opts.credentials.target_service_account() != "") {
    out.push_back(
        {"target_service_account"_nm = opts.credentials.target_service_account()});
  }

  if (opts.credentials.json_credentials() != "") {
    out.push_back({"json_credentials"_nm = opts.credentials.json_credentials()});
  }

  // GcsOptions direct members
  if (opts.endpoint_override != "") {
    out.push_back({"endpoint_override"_nm = opts.endpoint_override});
  }

  if (opts.scheme != "") {
    out.push_back({"scheme"_nm = opts.scheme});
  }

  if (opts.default_bucket_location != "") {
    out.push_back({"default_bucket_location"_nm = opts.default_bucket_location});
  }

  out.push_back({"retry_limit_seconds"_nm = opts.retry_limit_seconds.value()});

  // default_metadata
  if (opts.default_metadata != nullptr && opts.default_metadata->size() > 0) {
    cpp11::writable::strings metadata(opts.default_metadata->size());

    metadata.names() = opts.default_metadata->keys();

    for (int64_t i = 0; i < opts.default_metadata->size(); i++) {
      metadata[static_cast<size_t>(i)] = opts.default_metadata->value(i);
    }

    out.push_back({"default_metadata"_nm = metadata});
  }

  return out;
}

#endif
