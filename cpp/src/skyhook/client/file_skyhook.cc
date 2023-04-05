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

#include "skyhook/client/file_skyhook.h"
#include "skyhook/protocol/rados_protocol.h"
#include "skyhook/protocol/skyhook_protocol.h"

#include "arrow/compute/expression.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"

namespace skyhook {

class SkyhookFileFormat::Impl {
 public:
  Impl(std::shared_ptr<RadosConnCtx> ctx, std::string file_format)
      : ctx_(std::move(ctx)), file_format_(std::move(file_format)) {}

  ~Impl() = default;

  arrow::Status Init() {
    /// Connect to the RADOS cluster and instantiate a `SkyhookDirectObjectAccess`
    /// instance.
    auto connection = std::make_shared<skyhook::rados::RadosConn>(ctx_);
    RETURN_NOT_OK(connection->Connect());
    doa_ = std::make_shared<skyhook::SkyhookDirectObjectAccess>(connection);
    return arrow::Status::OK();
  }

  arrow::Result<arrow::RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<const SkyhookFileFormat>& format,
      const std::shared_ptr<arrow::dataset::ScanOptions>& options,
      const std::shared_ptr<arrow::dataset::FileFragment>& file) const {
    /// Convert string file format name to Enum.
    skyhook::SkyhookFileType::type file_format;
    if (file_format_ == "parquet") {
      file_format = skyhook::SkyhookFileType::type::PARQUET;
    } else if (file_format_ == "ipc") {
      file_format = skyhook::SkyhookFileType::type::IPC;
    } else {
      return arrow::Status::Invalid("Unsupported file format ", file_format_);
    }

    auto fut = arrow::DeferNotOk(options->io_context.executor()->Submit(
        [file, file_format, format,
         options]() -> arrow::Result<arrow::RecordBatchGenerator> {
          auto self = format->impl_.get();

          /// Retrieve the size of the file using POSIX `stat`.
          struct stat st {};
          RETURN_NOT_OK(self->doa_->Stat(file->source().path(), st));

          /// Create a ScanRequest instance.
          skyhook::ScanRequest req;
          req.filter_expression = options->filter;
          req.partition_expression = file->partition_expression();
          req.projection_schema = options->projected_schema;
          req.dataset_schema = options->dataset_schema;
          req.file_size = st.st_size;
          req.file_format = file_format;

          /// Serialize the ScanRequest into a ceph bufferlist.
          ceph::bufferlist request;
          RETURN_NOT_OK(skyhook::SerializeScanRequest(req, &request));

          /// Execute the Ceph object class method `scan_op`.
          ceph::bufferlist result;
          RETURN_NOT_OK(self->doa_->Exec(st.st_ino, "scan_op", request, result));

          /// Read RecordBatches from the result bufferlist. Since, this step might use
          /// threads for decompressing compressed batches, to avoid running into
          /// [ARROW-12597], we switch off threaded decompression to avoid nested
          /// threading scenarios when scan tasks are executed in parallel by the
          /// CpuThreadPool.
          arrow::RecordBatchVector batches;
          RETURN_NOT_OK(
              skyhook::DeserializeTable(result, !options->use_threads, &batches));
          auto gen = arrow::MakeVectorGenerator(std::move(batches));
          // Keep Ceph client alive
          arrow::RecordBatchGenerator gen_with_client = [format, gen]() { return gen(); };
          return gen_with_client;
        }));
    return arrow::MakeFromFuture(std::move(fut));
  }

  arrow::Result<std::shared_ptr<arrow::Schema>> Inspect(
      const arrow::dataset::FileSource& source) const {
    std::shared_ptr<arrow::dataset::FileFormat> file_format;
    /// Convert string file format name to Arrow FileFormat.
    if (file_format_ == "parquet") {
      file_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    } else if (file_format_ == "ipc") {
      file_format = std::make_shared<arrow::dataset::IpcFileFormat>();
    } else {
      return arrow::Status::Invalid("Unsupported file format ", file_format_);
    }
    std::shared_ptr<arrow::Schema> schema;
    ARROW_ASSIGN_OR_RAISE(schema, file_format->Inspect(source));
    return schema;
  }

 private:
  std::shared_ptr<skyhook::SkyhookDirectObjectAccess> doa_;
  std::shared_ptr<RadosConnCtx> ctx_;
  std::string file_format_;
};

arrow::Result<std::shared_ptr<SkyhookFileFormat>> SkyhookFileFormat::Make(
    std::shared_ptr<RadosConnCtx> ctx, std::string file_format) {
  auto format =
      std::make_shared<SkyhookFileFormat>(std::move(ctx), std::move(file_format));
  /// Establish connection to the Ceph cluster.
  RETURN_NOT_OK(format->Init());
  return format;
}

SkyhookFileFormat::SkyhookFileFormat(std::shared_ptr<RadosConnCtx> ctx,
                                     std::string file_format)
    : FileFormat(nullptr), impl_(new Impl(std::move(ctx), std::move(file_format))) {}

SkyhookFileFormat::~SkyhookFileFormat() = default;

arrow::Status SkyhookFileFormat::Init() { return impl_->Init(); }

arrow::Result<std::shared_ptr<arrow::Schema>> SkyhookFileFormat::Inspect(
    const arrow::dataset::FileSource& source) const {
  return impl_->Inspect(source);
}

arrow::Result<arrow::RecordBatchGenerator> SkyhookFileFormat::ScanBatchesAsync(
    const std::shared_ptr<arrow::dataset::ScanOptions>& options,
    const std::shared_ptr<arrow::dataset::FileFragment>& file) const {
  return impl_->ScanBatchesAsync(
      arrow::internal::checked_pointer_cast<const SkyhookFileFormat>(shared_from_this()),
      options, file);
}

std::shared_ptr<arrow::dataset::FileWriteOptions>
SkyhookFileFormat::DefaultWriteOptions() {
  return nullptr;
}

arrow::Result<std::shared_ptr<arrow::dataset::FileWriter>> SkyhookFileFormat::MakeWriter(
    std::shared_ptr<arrow::io::OutputStream> destination,
    std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<arrow::dataset::FileWriteOptions> options,
    arrow::fs::FileLocator destination_locator) const {
  return arrow::Status::NotImplemented("Skyhook writer not yet implemented.");
}

}  // namespace skyhook
