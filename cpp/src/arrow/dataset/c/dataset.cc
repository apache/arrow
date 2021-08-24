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

#include <iostream>

#include "arrow/c/bridge.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/c/api.h"
#include "arrow/dataset/c/helpers.h"
#include "arrow/util/logging.h"

const int kInspectAllFragments = arrow::dataset::InspectOptions::kInspectAllFragments;

namespace arrow {

namespace {

// convenience function to get an errno value from a status.
int ToCError(const Status& status) {
  if (ARROW_PREDICT_TRUE(status.ok())) {
    return 0;
  }

  switch (status.code()) {
    case StatusCode::IOError:
      return EIO;
    case StatusCode::NotImplemented:
      return ENOSYS;
    case StatusCode::OutOfMemory:
      return ENOMEM;
    default:
      return EINVAL;  // fallback for invalid, typeerror, etc.
  }
}

// convenience function to convert a C-style array of strings to a vector<std::string>
std::vector<std::string> to_string_vec(const char** vals, int n_vals) {
  std::vector<std::string> out;
  out.reserve(n_vals);
  const char** p = vals;
  for (int i = 0; i < n_vals; i++) {
    out.emplace_back(p[i]);
  }
  return out;
}

// Traits classes for use with templating a base Exported class to share
// implementation across them.

struct DatasetExportTraits {
  using CPPType = arrow::dataset::Dataset;
  using CType = struct Dataset;

  static constexpr auto IsReleasedFunc = &ArrowDatasetIsReleased;
  static constexpr auto MarkReleasedFunc = &ArrowDatasetMarkReleased;
};

struct DatasetFactoryExportTraits {
  using CPPType = arrow::dataset::DatasetFactory;
  using CType = struct DatasetFactory;

  static constexpr auto IsReleasedFunc = &ArrowDatasetFactoryIsReleased;
  static constexpr auto MarkReleasedFunc = &ArrowDatasetFactoryMarkReleased;
};

struct DatasetScannerExportTraits {
  using CPPType = arrow::dataset::Scanner;
  using CType = struct Scanner;

  static constexpr auto IsReleasedFunc = &ArrowScannerIsReleased;
  static constexpr auto MarkReleasedFunc = &ArrowScannerMarkReleased;
};

// base templated class for handling the C++ objects being managed by their
// C counterpart objects and basic functionality such as a "last_error" function.
template <typename Traits>
class ExportedDatasetType {
 public:
  using CPPType = typename Traits::CPPType;
  using CType = typename Traits::CType;

  struct PrivateData {
    explicit PrivateData(std::shared_ptr<CPPType> t) : exported_(std::move(t)) {}

    std::shared_ptr<CPPType> exported_;
    std::string last_error_;
    // for keeping random temporary strings alive long enough to return a const char*.
    std::string misc_string_;

    PrivateData() = default;
    ARROW_DISALLOW_COPY_AND_ASSIGN(PrivateData);
  };

  explicit ExportedDatasetType(CType* exported) : exported_(exported) {}

  void Release() {
    if (Traits::IsReleasedFunc(exported_)) {
      return;
    }
    DCHECK_NE(private_data(), nullptr);
    delete private_data();

    Traits::MarkReleasedFunc(exported_);
  }

  const char* GetLastError() {
    const auto& last_error = private_data()->last_error_;
    return last_error.empty() ? nullptr : last_error.c_str();
  }

  // C-compatible callbacks

  static const char* StaticGetLastError(CType* exported) {
    return ExportedDatasetType{exported}.GetLastError();
  }

  static void StaticRelease(CType* exported) { ExportedDatasetType{exported}.Release(); }

 protected:
  PrivateData* private_data() {
    return reinterpret_cast<PrivateData*>(exported_->private_data);
  }

  int CError(const Status& status) {
    if (ARROW_PREDICT_TRUE(status.ok())) {
      private_data()->last_error_.clear();
      return 0;
    }
    return ToCError(status);
  }

  const std::shared_ptr<CPPType>& exported() { return private_data()->exported_; }

 private:
  CType* exported_;
};

class ExportedScanner : public ExportedDatasetType<DatasetScannerExportTraits> {
 public:
  using ExportedDatasetType::ExportedDatasetType;

  // TODO(zeroshade): implement other Scanner functions like TakeRows, ToTable, CountRows,
  // and so on.

  Status ToRecordBatchStream(struct ArrowArrayStream* out) {
    ARROW_ASSIGN_OR_RAISE(auto reader, exported()->ToRecordBatchReader());
    return ExportRecordBatchReader(reader, out);
  }

  // C-compatible callbacks

  static int StaticToStream(struct Scanner* scanner, struct ArrowArrayStream* out) {
    ExportedScanner self{scanner};
    return self.CError(self.ToRecordBatchStream(out));
  }
};

Status ExportScanner(std::shared_ptr<arrow::dataset::Scanner> scanner,
                     struct Scanner* out) {
  out->release = ExportedScanner::StaticRelease;
  out->last_error = ExportedScanner::StaticGetLastError;
  out->to_stream = ExportedScanner::StaticToStream;
  out->private_data = new ExportedScanner::PrivateData{std::move(scanner)};
  return Status::OK();
}

class ExportedDataset : public ExportedDatasetType<DatasetExportTraits> {
 public:
  using ExportedDatasetType::ExportedDatasetType;

  Status GetSchema(struct ArrowSchema* out) {
    return ExportSchema(*exported()->schema(), out);
  }

  // TODO - implement filter functionality via expressions and predicates.
  //      - Expose option to decide whether UseThreads should be true, currently
  //      - it defaults to true.
  //      - Expose Fragment Readahead options
  //      - Expose other fragment scan options
  Status NewScan(const char** columns, const int n_cols, int64_t batch_size,
                 struct Scanner* out) {
    ARROW_ASSIGN_OR_RAISE(auto builder, exported()->NewScan());

    auto col_vector = to_string_vec(columns, n_cols);
    if (!col_vector.empty()) {
      RETURN_NOT_OK(builder->Project(col_vector));
    }
    if (batch_size > 0) {
      RETURN_NOT_OK(builder->BatchSize(batch_size));
    }
    RETURN_NOT_OK(builder->UseThreads(true));

    ARROW_ASSIGN_OR_RAISE(auto scanner, builder->Finish());
    return ExportScanner(scanner, out);
  }

  // C-compatible callbacks

  static const char* StaticGetDatasetTypeName(struct Dataset* dataset) {
    ExportedDataset self{dataset};
    self.private_data()->misc_string_ = self.exported()->type_name();
    return self.private_data()->misc_string_.c_str();
  }

  static int StaticGetSchema(struct Dataset* dataset, struct ArrowSchema* out) {
    ExportedDataset self{dataset};
    return self.CError(self.GetSchema(out));
  }

  static int StaticNewScan(struct Dataset* dataset, const char** columns,
                           const int n_cols, int64_t batch_size, struct Scanner* out) {
    ExportedDataset self{dataset};
    return self.CError(self.NewScan(columns, n_cols, batch_size, out));
  }
};

Status ExportDataset(std::shared_ptr<arrow::dataset::Dataset> dataset,
                     struct Dataset* out) {
  out->release = ExportedDataset::StaticRelease;
  out->last_error = ExportedDataset::StaticGetLastError;
  out->get_dataset_type_name = ExportedDataset::StaticGetDatasetTypeName;
  out->get_schema = ExportedDataset::StaticGetSchema;
  out->new_scan = ExportedDataset::StaticNewScan;
  out->private_data = new ExportedDataset::PrivateData{std::move(dataset)};
  return Status::OK();
}

// TODO(zeroshade): expose partition expression, GetFragments and ReplaceSchema
class ExportedDatasetFactory : public ExportedDatasetType<DatasetFactoryExportTraits> {
 public:
  using ExportedDatasetType::ExportedDatasetType;

  Status GetSchema(const int inspect_num_fragments, struct ArrowSchema* out_schema) {
    arrow::dataset::InspectOptions opts;
    opts.fragments = inspect_num_fragments;

    ARROW_ASSIGN_OR_RAISE(auto schema, factory()->Inspect(opts));
    return ExportSchema(*schema, out_schema);
  }

  Status CreateDataset(struct Dataset* out) {
    ARROW_ASSIGN_OR_RAISE(auto ds, factory()->Finish());
    return ExportDataset(ds, out);
  }

  // C-compatible callbacks

  static int StaticInspectSchema(struct DatasetFactory* factory,
                                 const int inspect_num_fragments,
                                 struct ArrowSchema* out_schema) {
    ExportedDatasetFactory self{factory};
    return self.CError(self.GetSchema(inspect_num_fragments, out_schema));
  }

  static int StaticCreateDataset(struct DatasetFactory* factory, struct Dataset* out) {
    ExportedDatasetFactory self{factory};
    return self.CError(self.CreateDataset(out));
  }

 private:
  const std::shared_ptr<arrow::dataset::DatasetFactory>& factory() {
    return private_data()->exported_;
  }
};

Status ExportDatasetFactory(std::shared_ptr<arrow::dataset::DatasetFactory> factory,
                            struct DatasetFactory* out) {
  out->release = ExportedDatasetFactory::StaticRelease;
  out->inspect_schema = ExportedDatasetFactory::StaticInspectSchema;
  out->create_dataset = ExportedDatasetFactory::StaticCreateDataset;
  out->last_error = ExportedDatasetFactory::StaticGetLastError;
  out->private_data = new ExportedDatasetFactory::PrivateData{std::move(factory)};
  return Status::OK();
}

}  // namespace

}  // namespace arrow

namespace {

arrow::Result<std::shared_ptr<arrow::dataset::FileFormat>> get_file_format(
    const int format) {
  switch (format) {
    case DS_PARQUET_FORMAT:
      return std::make_shared<arrow::dataset::ParquetFileFormat>();
    case DS_CSV_FORMAT:
      return std::make_shared<arrow::dataset::CsvFileFormat>();
    case DS_IPC_FORMAT:
      return std::make_shared<arrow::dataset::IpcFileFormat>();
    default:
      std::string error_message = "illegal file format id: " + std::to_string(format);
      return arrow::Status::Invalid(error_message);
  }
}

}  // namespace

int dataset_factory_from_path(const char* uri, const int file_format_id,
                              struct DatasetFactory* out) {
  auto file_format = get_file_format(file_format_id);
  if (!file_format.ok()) {
    return EINVAL;
  }

  auto df = arrow::dataset::FileSystemDatasetFactory::Make(
      std::string(uri), file_format.MoveValueUnsafe(),
      arrow::dataset::FileSystemFactoryOptions{});
  if (!df.ok()) {
    return arrow::ToCError(df.status());
  }

  return arrow::ToCError(arrow::ExportDatasetFactory(df.MoveValueUnsafe(), out));
}

// TODO(zeroshade): add other filesystem dataset options such as directory searches
// and other dataset types such as inmemory and union datasets.
