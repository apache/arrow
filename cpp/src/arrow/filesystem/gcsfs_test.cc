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

#include "arrow/filesystem/gcsfs.h"

#include <absl/time/time.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <google/cloud/credentials.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/options.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/process.hpp>
#include <random>
#include <string>

#include "arrow/filesystem/gcsfs_internal.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace fs {
namespace {

namespace bp = boost::process;
namespace gc = google::cloud;
namespace gcs = google::cloud::storage;

using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::NotNull;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

auto const* kPreexistingBucket = "test-bucket-name";
auto const* kPreexistingObject = "test-object-name";
auto const* kLoremIpsum = R"""(
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu
fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
culpa qui officia deserunt mollit anim id est laborum.
)""";

class GcsIntegrationTest : public ::testing::Test {
 public:
  ~GcsIntegrationTest() override {
    if (server_process_.valid()) {
      // Brutal shutdown
      server_process_.terminate();
      server_process_.wait();
    }
  }

 protected:
  void SetUp() override {
    // Initialize a PRNG with a small amount of entropy.
    generator_ = std::mt19937_64(std::random_device()());
    port_ = std::to_string(GetListenPort());
    auto exe_path = bp::search_path("python3");
    ASSERT_THAT(exe_path, Not(IsEmpty()));

    server_process_ = bp::child(boost::this_process::environment(), exe_path, "-m",
                                "testbench", "--port", port_);

    // Create a bucket and a small file in the testbench. This makes it easier to
    // bootstrap GcsFileSystem and its tests.
    auto client = gcs::Client(
        google::cloud::Options{}
            .set<gcs::RestEndpointOption>("http://127.0.0.1:" + port_)
            .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials()));
    google::cloud::StatusOr<gcs::BucketMetadata> bucket = client.CreateBucketForProject(
        kPreexistingBucket, "ignored-by-testbench", gcs::BucketMetadata{});
    ASSERT_TRUE(bucket.ok()) << "Failed to create bucket <" << kPreexistingBucket
                             << ">, status=" << bucket.status();

    google::cloud::StatusOr<gcs::ObjectMetadata> object =
        client.InsertObject(kPreexistingBucket, kPreexistingObject, kLoremIpsum);
    ASSERT_TRUE(object.ok()) << "Failed to create object <" << kPreexistingObject
                             << ">, status=" << object.status();
  }

  static std::string PreexistingObjectPath() {
    return std::string(kPreexistingBucket) + "/" + kPreexistingObject;
  }

  static std::string NotFoundObjectPath() {
    return std::string(kPreexistingBucket) + "/not-found";
  }

  GcsOptions TestGcsOptions() {
    GcsOptions options;
    options.endpoint_override = "127.0.0.1:" + port_;
    options.scheme = "http";
    return options;
  }

  gcs::Client GcsClient() {
    return gcs::Client(
        google::cloud::Options{}
            .set<gcs::RestEndpointOption>("http://127.0.0.1:" + port_)
            .set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials()));
  }

  std::string RandomLine(int lineno, std::size_t width) {
    auto const fillers = std::string("abcdefghijlkmnopqrstuvwxyz0123456789");
    std::uniform_int_distribution<std::size_t> d(0, fillers.size() - 1);

    auto line = std::to_string(lineno) + ":    ";
    std::generate_n(std::back_inserter(line), width - line.size() - 1,
                    [&] { return fillers[d(generator_)]; });
    line += '\n';
    return line;
  }

  int RandomIndex(std::size_t end) {
    return std::uniform_int_distribution<std::size_t>(0, end - 1)(generator_);
  }

 private:
  std::mt19937_64 generator_;
  std::string port_;
  bp::child server_process_;
};

TEST(GcsFileSystem, OptionsCompare) {
  GcsOptions a;
  GcsOptions b;
  b.endpoint_override = "localhost:1234";
  EXPECT_TRUE(a.Equals(a));
  EXPECT_TRUE(b.Equals(b));
  auto c = b;
  c.scheme = "http";
  EXPECT_FALSE(b.Equals(c));
}

TEST(GcsFileSystem, ToArrowStatusOK) {
  Status actual = internal::ToArrowStatus(google::cloud::Status());
  EXPECT_TRUE(actual.ok());
}

TEST(GcsFileSystem, ToArrowStatus) {
  struct {
    google::cloud::StatusCode input;
    arrow::StatusCode expected;
  } cases[] = {
      {google::cloud::StatusCode::kCancelled, StatusCode::Cancelled},
      {google::cloud::StatusCode::kUnknown, StatusCode::UnknownError},
      {google::cloud::StatusCode::kInvalidArgument, StatusCode::Invalid},
      {google::cloud::StatusCode::kDeadlineExceeded, StatusCode::IOError},
      {google::cloud::StatusCode::kNotFound, StatusCode::IOError},
      {google::cloud::StatusCode::kAlreadyExists, StatusCode::AlreadyExists},
      {google::cloud::StatusCode::kPermissionDenied, StatusCode::IOError},
      {google::cloud::StatusCode::kUnauthenticated, StatusCode::IOError},
      {google::cloud::StatusCode::kResourceExhausted, StatusCode::CapacityError},
      {google::cloud::StatusCode::kFailedPrecondition, StatusCode::IOError},
      {google::cloud::StatusCode::kAborted, StatusCode::IOError},
      {google::cloud::StatusCode::kOutOfRange, StatusCode::Invalid},
      {google::cloud::StatusCode::kUnimplemented, StatusCode::NotImplemented},
      {google::cloud::StatusCode::kInternal, StatusCode::IOError},
      {google::cloud::StatusCode::kUnavailable, StatusCode::IOError},
      {google::cloud::StatusCode::kDataLoss, StatusCode::IOError},
  };

  for (const auto& test : cases) {
    auto status = google::cloud::Status(test.input, "test-message");
    auto message = [&] {
      std::ostringstream os;
      os << status;
      return os.str();
    }();
    SCOPED_TRACE("Testing with status=" + message);
    const auto actual = arrow::fs::internal::ToArrowStatus(status);
    EXPECT_EQ(actual.code(), test.expected);
    EXPECT_THAT(actual.message(), HasSubstr(message));
  }
}

TEST(GcsFileSystem, FileSystemCompare) {
  GcsOptions a_options;
  a_options.scheme = "http";
  auto a = internal::MakeGcsFileSystemForTest(a_options);
  EXPECT_THAT(a, NotNull());
  EXPECT_TRUE(a->Equals(*a));

  GcsOptions b_options;
  b_options.scheme = "http";
  b_options.endpoint_override = "localhost:1234";
  auto b = internal::MakeGcsFileSystemForTest(b_options);
  EXPECT_THAT(b, NotNull());
  EXPECT_TRUE(b->Equals(*b));

  EXPECT_FALSE(a->Equals(*b));
}

std::shared_ptr<const KeyValueMetadata> KeyValueMetadataForTest() {
  return arrow::key_value_metadata({
      {"Cache-Control", "test-only-cache-control"},
      {"Content-Disposition", "test-only-content-disposition"},
      {"Content-Encoding", "test-only-content-encoding"},
      {"Content-Language", "test-only-content-language"},
      {"Content-Type", "test-only-content-type"},
      {"customTime", "2021-10-26T01:02:03.456Z"},
      {"storageClass", "test-only-storage-class"},
      {"key", "test-only-value"},
      {"kmsKeyName", "test-only-kms-key-name"},
      {"predefinedAcl", "test-only-predefined-acl"},
      // computed with: /bin/echo -n "01234567" | openssl base64
      {"encryptionKeyBase64", "MDEyMzQ1Njc="},
  });
}

TEST(GcsFileSystem, ToEncryptionKey) {
  gcs::EncryptionKey key;
  ASSERT_OK_AND_ASSIGN(key,
                       arrow::fs::internal::ToEncryptionKey(KeyValueMetadataForTest()));
  ASSERT_TRUE(key.has_value());
  EXPECT_EQ(key.value().algorithm, "AES256");
  EXPECT_EQ(key.value().key, "MDEyMzQ1Njc=");
  // /bin/echo -n "01234567" | sha256sum | awk '{printf("%s", $1);}' |
  //       xxd -r -p | openssl base64
  // to get the SHA256 value of the key.
  EXPECT_EQ(key.value().sha256, "kkWSubED8U+DP6r7Z/SAaR8BmIqkV8AGF2n1jNRzEbw=");
}

TEST(GcsFileSystem, ToEncryptionKeyEmpty) {
  gcs::EncryptionKey key;
  ASSERT_OK_AND_ASSIGN(key, arrow::fs::internal::ToEncryptionKey({}));
  ASSERT_FALSE(key.has_value());
}

TEST(GcsFileSystem, ToKmsKeyName) {
  gcs::KmsKeyName key;
  ASSERT_OK_AND_ASSIGN(key, arrow::fs::internal::ToKmsKeyName(KeyValueMetadataForTest()));
  EXPECT_EQ(key.value_or(""), "test-only-kms-key-name");
}

TEST(GcsFileSystem, ToKmsKeyNameEmpty) {
  gcs::KmsKeyName key;
  ASSERT_OK_AND_ASSIGN(key, arrow::fs::internal::ToKmsKeyName({}));
  ASSERT_FALSE(key.has_value());
}

TEST(GcsFileSystem, ToPredefinedAcl) {
  gcs::PredefinedAcl predefined;
  ASSERT_OK_AND_ASSIGN(predefined,
                       arrow::fs::internal::ToPredefinedAcl(KeyValueMetadataForTest()));
  EXPECT_EQ(predefined.value_or(""), "test-only-predefined-acl");
}

TEST(GcsFileSystem, ToPredefinedAclEmpty) {
  gcs::PredefinedAcl predefined;
  ASSERT_OK_AND_ASSIGN(predefined, arrow::fs::internal::ToPredefinedAcl({}));
  ASSERT_FALSE(predefined.has_value());
}

TEST(GcsFileSystem, ToObjectMetadata) {
  gcs::WithObjectMetadata metadata;
  ASSERT_OK_AND_ASSIGN(metadata,
                       arrow::fs::internal::ToObjectMetadata(KeyValueMetadataForTest()));
  ASSERT_TRUE(metadata.has_value());
  EXPECT_EQ(metadata.value().cache_control(), "test-only-cache-control");
  EXPECT_EQ(metadata.value().content_disposition(), "test-only-content-disposition");
  EXPECT_EQ(metadata.value().content_encoding(), "test-only-content-encoding");
  EXPECT_EQ(metadata.value().content_language(), "test-only-content-language");
  EXPECT_EQ(metadata.value().content_type(), "test-only-content-type");
  ASSERT_TRUE(metadata.value().has_custom_time());
  EXPECT_THAT(metadata.value().metadata(),
              UnorderedElementsAre(Pair("key", "test-only-value")));
}

TEST(GcsFileSystem, ToObjectMetadataEmpty) {
  gcs::WithObjectMetadata metadata;
  ASSERT_OK_AND_ASSIGN(metadata, arrow::fs::internal::ToObjectMetadata({}));
  ASSERT_FALSE(metadata.has_value());
}

TEST(GcsFileSystem, ToObjectMetadataInvalidCustomTime) {
  auto metadata = arrow::fs::internal::ToObjectMetadata(arrow::key_value_metadata({
      {"customTime", "invalid"},
  }));
  EXPECT_EQ(metadata.status().code(), StatusCode::Invalid);
  EXPECT_THAT(metadata.status().message(), HasSubstr("Error parsing RFC-3339"));
}

TEST(GcsFileSystem, ObjectMetadataRoundtrip) {
  const auto source = KeyValueMetadataForTest();
  gcs::WithObjectMetadata tmp;
  ASSERT_OK_AND_ASSIGN(tmp, arrow::fs::internal::ToObjectMetadata(source));
  std::shared_ptr<const KeyValueMetadata> destination;
  ASSERT_OK_AND_ASSIGN(destination, arrow::fs::internal::FromObjectMetadata(tmp.value()));
  // Only a subset of the keys are configurable in gcs::ObjectMetadata, for example, the
  // size and CRC32C values of an object are only settable when the metadata is received
  // from the services. For those keys that should be preserved, verify they are preserved
  // in the round trip.
  for (auto key : {"Cache-Control", "Content-Disposition", "Content-Encoding",
                   "Content-Language", "Content-Type", "storageClass"}) {
    SCOPED_TRACE("Testing key " + std::string(key));
    ASSERT_OK_AND_ASSIGN(auto s, source->Get(key));
    ASSERT_OK_AND_ASSIGN(auto d, destination->Get(key));
    EXPECT_EQ(s, d);
  }
  // RFC-3339 formatted timestamps may differ in trivial things, e.g., the UTC timezone
  // can be represented as 'Z' or '+00:00'.
  ASSERT_OK_AND_ASSIGN(auto s, source->Get("customTime"));
  ASSERT_OK_AND_ASSIGN(auto d, destination->Get("customTime"));
  std::string err;
  absl::Time ts;
  absl::Time td;
  ASSERT_TRUE(absl::ParseTime(absl::RFC3339_full, s, &ts, &err)) << "error=" << err;
  ASSERT_TRUE(absl::ParseTime(absl::RFC3339_full, d, &td, &err)) << "error=" << err;
  EXPECT_NEAR(absl::ToDoubleSeconds(ts - td), 0, 0.5);
}

TEST_F(GcsIntegrationTest, GetFileInfoBucket) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  arrow::fs::AssertFileInfo(fs.get(), kPreexistingBucket, FileType::Directory);
}

TEST_F(GcsIntegrationTest, GetFileInfoObject) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::File);
}

TEST_F(GcsIntegrationTest, DeleteRootDirContents) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented, HasSubstr("too dangerous"),
                                  fs->DeleteRootDirContents());
}

TEST_F(GcsIntegrationTest, DeleteFileSuccess) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_OK(fs->DeleteFile(PreexistingObjectPath()));
  arrow::fs::AssertFileInfo(fs.get(), PreexistingObjectPath(), FileType::NotFound);
}

TEST_F(GcsIntegrationTest, DeleteFileFailure) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  ASSERT_RAISES(IOError, fs->DeleteFile(NotFoundObjectPath()));
}

TEST_F(GcsIntegrationTest, DeleteFileDirectoryFails) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto path = std::string(kPreexistingBucket) + "/DeleteFileDirectoryFails/";
  ASSERT_RAISES(IOError, fs->DeleteFile(path));
}

TEST_F(GcsIntegrationTest, CopyFileSuccess) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto destination_path = kPreexistingBucket + std::string("/copy-destination");
  ASSERT_OK(fs->CopyFile(PreexistingObjectPath(), destination_path));
  arrow::fs::AssertFileInfo(fs.get(), destination_path, FileType::File);
}

TEST_F(GcsIntegrationTest, CopyFileNotFound) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  const auto destination_path = kPreexistingBucket + std::string("/copy-destination");
  ASSERT_RAISES(IOError, fs->CopyFile(NotFoundObjectPath(), destination_path));
}

TEST_F(GcsIntegrationTest, ReadObjectString) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs->OpenInputStream(PreexistingObjectPath()));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, stream->Read(buffer.size(), buffer.data()));

  EXPECT_EQ(std::string(buffer.data(), size), kLoremIpsum);
}

TEST_F(GcsIntegrationTest, ReadObjectStringBuffers) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs->OpenInputStream(PreexistingObjectPath()));

  std::string contents;
  std::shared_ptr<Buffer> buffer;
  do {
    ASSERT_OK_AND_ASSIGN(buffer, stream->Read(16));
    contents.append(buffer->ToString());
  } while (buffer && buffer->size() != 0);

  EXPECT_EQ(contents, kLoremIpsum);
}

TEST_F(GcsIntegrationTest, ReadObjectInfo) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingObjectPath()));

  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs->OpenInputStream(info));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, stream->Read(buffer.size(), buffer.data()));

  EXPECT_EQ(std::string(buffer.data(), size), kLoremIpsum);
}

TEST_F(GcsIntegrationTest, ReadObjectNotFound) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  ASSERT_RAISES(IOError, fs->OpenInputStream(NotFoundObjectPath()));
}

TEST_F(GcsIntegrationTest, ReadObjectInfoInvalid) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(kPreexistingBucket));
  ASSERT_RAISES(IOError, fs->OpenInputStream(info));

  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs->OpenInputStream(info));
}

TEST_F(GcsIntegrationTest, ReadObjectReadMetadata) {
  auto client = GcsClient();
  const auto custom_time = std::chrono::system_clock::now() + std::chrono::hours(1);
  const std::string object_name = "ReadObjectMetadataTest/simple.txt";
  const gcs::ObjectMetadata expected =
      client
          .InsertObject(kPreexistingBucket, object_name,
                        "The quick brown fox jumps over the lazy dog",
                        gcs::WithObjectMetadata(gcs::ObjectMetadata()
                                                    .set_content_type("text/plain")
                                                    .set_custom_time(custom_time)
                                                    .set_cache_control("no-cache")
                                                    .upsert_metadata("key0", "value0")))
          .value();

  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());
  std::shared_ptr<io::InputStream> stream;
  ASSERT_OK_AND_ASSIGN(
      stream, fs->OpenInputStream(std::string(kPreexistingBucket) + "/" + object_name));

  auto format_time = [](std::chrono::system_clock::time_point tp) {
    return absl::FormatTime(absl::RFC3339_full, absl::FromChrono(tp),
                            absl::UTCTimeZone());
  };

  std::shared_ptr<const KeyValueMetadata> actual;
  ASSERT_OK_AND_ASSIGN(actual, stream->ReadMetadata());
  ASSERT_OK_AND_EQ(expected.self_link(), actual->Get("selfLink"));
  ASSERT_OK_AND_EQ(expected.name(), actual->Get("name"));
  ASSERT_OK_AND_EQ(expected.bucket(), actual->Get("bucket"));
  ASSERT_OK_AND_EQ(std::to_string(expected.generation()), actual->Get("generation"));
  ASSERT_OK_AND_EQ(expected.content_type(), actual->Get("Content-Type"));
  ASSERT_OK_AND_EQ(format_time(expected.time_created()), actual->Get("timeCreated"));
  ASSERT_OK_AND_EQ(format_time(expected.updated()), actual->Get("updated"));
  ASSERT_FALSE(actual->Contains("timeDeleted"));
  ASSERT_OK_AND_EQ(format_time(custom_time), actual->Get("customTime"));
  ASSERT_OK_AND_EQ("false", actual->Get("temporaryHold"));
  ASSERT_OK_AND_EQ("false", actual->Get("eventBasedHold"));
  ASSERT_FALSE(actual->Contains("retentionExpirationTime"));
  ASSERT_OK_AND_EQ(expected.storage_class(), actual->Get("storageClass"));
  ASSERT_FALSE(actual->Contains("storageClassUpdated"));
  ASSERT_OK_AND_EQ(std::to_string(expected.size()), actual->Get("size"));
  ASSERT_OK_AND_EQ(expected.md5_hash(), actual->Get("md5Hash"));
  ASSERT_OK_AND_EQ(expected.media_link(), actual->Get("mediaLink"));
  ASSERT_OK_AND_EQ(expected.content_encoding(), actual->Get("Content-Encoding"));
  ASSERT_OK_AND_EQ(expected.content_disposition(), actual->Get("Content-Disposition"));
  ASSERT_OK_AND_EQ(expected.content_language(), actual->Get("Content-Language"));
  ASSERT_OK_AND_EQ(expected.cache_control(), actual->Get("Cache-Control"));
  auto p = expected.metadata().find("key0");
  ASSERT_TRUE(p != expected.metadata().end());
  ASSERT_OK_AND_EQ(p->second, actual->Get("metadata.key0"));
  ASSERT_EQ(expected.has_owner(), actual->Contains("owner.entity"));
  ASSERT_EQ(expected.has_owner(), actual->Contains("owner.entityId"));
  ASSERT_OK_AND_EQ(expected.crc32c(), actual->Get("crc32c"));
  ASSERT_OK_AND_EQ(std::to_string(expected.component_count()),
                   actual->Get("componentCount"));
  ASSERT_OK_AND_EQ(expected.etag(), actual->Get("etag"));
  ASSERT_FALSE(actual->Contains("customerEncryption.encryptionAlgorithm"));
  ASSERT_FALSE(actual->Contains("customerEncryption.keySha256"));
  ASSERT_FALSE(actual->Contains("kmsKeyName"));
}

TEST_F(GcsIntegrationTest, WriteObjectSmall) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  const auto path = kPreexistingBucket + std::string("/test-write-object");
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  const auto expected = std::string(kLoremIpsum);
  ASSERT_OK(output->Write(expected.data(), expected.size()));
  ASSERT_OK(output->Close());

  // Verify we can read the object back.
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK_AND_ASSIGN(input, fs->OpenInputStream(path));

  std::array<char, 1024> inbuf{};
  std::int64_t size;
  ASSERT_OK_AND_ASSIGN(size, input->Read(inbuf.size(), inbuf.data()));

  EXPECT_EQ(std::string(inbuf.data(), size), expected);
}

TEST_F(GcsIntegrationTest, WriteObjectLarge) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  const auto path = kPreexistingBucket + std::string("/test-write-object");
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  // These buffer sizes are intentionally not multiples of the upload quantum (256 KiB).
  std::array<std::int64_t, 3> sizes{257 * 1024, 258 * 1024, 259 * 1024};
  std::array<std::string, 3> buffers{
      std::string(sizes[0], 'A'),
      std::string(sizes[1], 'B'),
      std::string(sizes[2], 'C'),
  };
  auto expected = std::int64_t{0};
  for (auto i = 0; i != 3; ++i) {
    ASSERT_OK(output->Write(buffers[i].data(), buffers[i].size()));
    expected += sizes[i];
    ASSERT_EQ(output->Tell(), expected);
  }
  ASSERT_OK(output->Close());

  // Verify we can read the object back.
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK_AND_ASSIGN(input, fs->OpenInputStream(path));

  std::string contents;
  std::shared_ptr<Buffer> buffer;
  do {
    ASSERT_OK_AND_ASSIGN(buffer, input->Read(128 * 1024));
    ASSERT_TRUE(buffer);
    contents.append(buffer->ToString());
  } while (buffer->size() != 0);

  EXPECT_EQ(contents, buffers[0] + buffers[1] + buffers[2]);
}

TEST_F(GcsIntegrationTest, OpenInputFileMixedReadVsReadAt) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  // Create a file large enough to make the random access tests non-trivial.
  auto constexpr kLineWidth = 100;
  auto constexpr kLineCount = 4096;
  std::vector<std::string> lines(kLineCount);
  int lineno = 0;
  std::generate_n(lines.begin(), lines.size(),
                  [&] { return RandomLine(++lineno, kLineWidth); });

  const auto path =
      kPreexistingBucket + std::string("/OpenInputFileMixedReadVsReadAt/object-name");
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  for (auto const& line : lines) {
    ASSERT_OK(output->Write(line.data(), line.size()));
  }
  ASSERT_OK(output->Close());

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
  for (int i = 0; i != 32; ++i) {
    SCOPED_TRACE("Iteration " + std::to_string(i));
    // Verify sequential reads work as expected.
    std::array<char, kLineWidth> buffer{};
    std::int64_t size;
    {
      ASSERT_OK_AND_ASSIGN(auto actual, file->Read(kLineWidth));
      EXPECT_EQ(lines[2 * i], actual->ToString());
    }
    {
      ASSERT_OK_AND_ASSIGN(size, file->Read(buffer.size(), buffer.data()));
      EXPECT_EQ(size, kLineWidth);
      auto actual = std::string{buffer.begin(), buffer.end()};
      EXPECT_EQ(lines[2 * i + 1], actual);
    }

    // Verify random reads interleave too.
    auto const index = RandomIndex(kLineCount);
    auto const position = index * kLineWidth;
    ASSERT_OK_AND_ASSIGN(size, file->ReadAt(position, buffer.size(), buffer.data()));
    EXPECT_EQ(size, kLineWidth);
    auto actual = std::string{buffer.begin(), buffer.end()};
    EXPECT_EQ(lines[index], actual);

    // Verify random reads using buffers work.
    ASSERT_OK_AND_ASSIGN(auto b, file->ReadAt(position, kLineWidth));
    EXPECT_EQ(lines[index], b->ToString());
  }
}

TEST_F(GcsIntegrationTest, OpenInputFileRandomSeek) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  // Create a file large enough to make the random access tests non-trivial.
  auto constexpr kLineWidth = 100;
  auto constexpr kLineCount = 4096;
  std::vector<std::string> lines(kLineCount);
  int lineno = 0;
  std::generate_n(lines.begin(), lines.size(),
                  [&] { return RandomLine(++lineno, kLineWidth); });

  const auto path =
      kPreexistingBucket + std::string("/OpenInputFileRandomSeek/object-name");
  std::shared_ptr<io::OutputStream> output;
  ASSERT_OK_AND_ASSIGN(output, fs->OpenOutputStream(path, {}));
  for (auto const& line : lines) {
    ASSERT_OK(output->Write(line.data(), line.size()));
  }
  ASSERT_OK(output->Close());

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(path));
  for (int i = 0; i != 32; ++i) {
    SCOPED_TRACE("Iteration " + std::to_string(i));
    // Verify sequential reads work as expected.
    auto const index = RandomIndex(kLineCount);
    auto const position = index * kLineWidth;
    ASSERT_OK(file->Seek(position));
    ASSERT_OK_AND_ASSIGN(auto actual, file->Read(kLineWidth));
    EXPECT_EQ(lines[index], actual->ToString());
  }
}

TEST_F(GcsIntegrationTest, OpenInputFileInfo) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(PreexistingObjectPath()));

  std::shared_ptr<io::RandomAccessFile> file;
  ASSERT_OK_AND_ASSIGN(file, fs->OpenInputFile(info));

  std::array<char, 1024> buffer{};
  std::int64_t size;
  auto constexpr kStart = 16;
  ASSERT_OK_AND_ASSIGN(size, file->ReadAt(kStart, buffer.size(), buffer.data()));

  auto const expected = std::string(kLoremIpsum).substr(kStart);
  EXPECT_EQ(std::string(buffer.data(), size), expected);
}

TEST_F(GcsIntegrationTest, OpenInputFileNotFound) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  ASSERT_RAISES(IOError, fs->OpenInputFile(NotFoundObjectPath()));
}

TEST_F(GcsIntegrationTest, OpenInputFileInfoInvalid) {
  auto fs = internal::MakeGcsFileSystemForTest(TestGcsOptions());

  arrow::fs::FileInfo info;
  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(kPreexistingBucket));
  ASSERT_RAISES(IOError, fs->OpenInputFile(info));

  ASSERT_OK_AND_ASSIGN(info, fs->GetFileInfo(NotFoundObjectPath()));
  ASSERT_RAISES(IOError, fs->OpenInputFile(info));
}

}  // namespace
}  // namespace fs
}  // namespace arrow
