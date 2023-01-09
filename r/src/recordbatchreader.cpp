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

#include <arrow/ipc/reader.h>
#include <arrow/table.h>

// [[arrow::export]]
std::shared_ptr<arrow::Schema> RecordBatchReader__schema(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return reader->schema();
}

// [[arrow::export]]
void RecordBatchReader__Close(const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return arrow::StopIfNotOk(reader->Close());
}

// [[arrow::export]]
void RecordBatchReader__UnsafeDelete(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  auto& reader_unsafe = const_cast<std::shared_ptr<arrow::RecordBatchReader>&>(reader);
  reader_unsafe.reset();
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatchReader__ReadNext(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  std::shared_ptr<arrow::RecordBatch> batch;
  StopIfNotOk(reader->ReadNext(&batch));
  return batch;
}

// [[arrow::export]]
cpp11::list RecordBatchReader__batches(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return arrow::r::to_r_list(ValueOrStop(reader->ToRecordBatches()));
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatchReader> RecordBatchReader__from_batches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    cpp11::sexp schema_sxp) {
  bool infer_schema = !Rf_inherits(schema_sxp, "Schema");

  if (infer_schema) {
    return ValueOrStop(arrow::RecordBatchReader::Make(std::move(batches)));
  } else {
    auto schema = cpp11::as_cpp<std::shared_ptr<arrow::Schema>>(schema_sxp);
    return ValueOrStop(arrow::RecordBatchReader::Make(std::move(batches), schema));
  }
}

class RFunctionRecordBatchReader : public arrow::RecordBatchReader {
 public:
  RFunctionRecordBatchReader(cpp11::sexp fun,
                             const std::shared_ptr<arrow::Schema>& schema)
      : fun_(fun), schema_(schema) {}

  std::shared_ptr<arrow::Schema> schema() const { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch_out) {
    auto batch = SafeCallIntoR<std::shared_ptr<arrow::RecordBatch>>([&]() {
      cpp11::sexp result_sexp = cpp11::function(fun_)();
      if (result_sexp == R_NilValue) {
        return std::shared_ptr<arrow::RecordBatch>(nullptr);
      } else if (!Rf_inherits(result_sexp, "RecordBatch")) {
        cpp11::stop("Expected fun() to return an arrow::RecordBatch");
      }

      return cpp11::as_cpp<std::shared_ptr<arrow::RecordBatch>>(result_sexp);
    });

    RETURN_NOT_OK(batch);

    if (batch.ValueUnsafe().get() != nullptr &&
        !batch.ValueUnsafe()->schema()->Equals(schema_)) {
      return arrow::Status::Invalid("Expected fun() to return batch with schema '",
                                    schema_->ToString(), "' but got batch with schema '",
                                    batch.ValueUnsafe()->schema()->ToString(), "'");
    }

    *batch_out = batch.ValueUnsafe();
    return arrow::Status::OK();
  }

 private:
  cpp11::sexp fun_;
  std::shared_ptr<arrow::Schema> schema_;
};

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatchReader> RecordBatchReader__from_function(
    cpp11::sexp fun_sexp, const std::shared_ptr<arrow::Schema>& schema) {
  return std::make_shared<RFunctionRecordBatchReader>(fun_sexp, schema);
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatchReader> RecordBatchReader__from_Table(
    const std::shared_ptr<arrow::Table>& table) {
  return std::make_shared<arrow::TableBatchReader>(table);
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_RecordBatchReader(
    const std::shared_ptr<arrow::RecordBatchReader>& reader) {
  return ValueOrStop(reader->ToTable());
}

// Because the head() operation can leave a RecordBatchReader whose contents
// will never be drained, we implement a wrapper class here that takes care
// to (1) return only the requested number of rows (or fewer) and (2) Close
// and release the underlying reader as soon as possible. This is mostly
// useful for the ExecPlanReader, whose Close() method also requests
// that the ExecPlan stop producing, but may also be useful for readers
// that point to an open file and whose Close() or delete method releases
// the file.
class RecordBatchReaderHead : public arrow::RecordBatchReader {
 public:
  RecordBatchReaderHead(std::shared_ptr<arrow::RecordBatchReader> reader,
                        int64_t num_rows)
      : done_(false), schema_(reader->schema()), reader_(reader), num_rows_(num_rows) {}

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch_out) override {
    if (done_) {
      // Close() has been called
      batch_out = nullptr;
      return arrow::Status::OK();
    }

    ARROW_RETURN_NOT_OK(reader_->ReadNext(batch_out));
    if (batch_out->get()) {
      num_rows_ -= batch_out->get()->num_rows();
      if (num_rows_ < 0) {
        auto smaller_batch =
            batch_out->get()->Slice(0, batch_out->get()->num_rows() + num_rows_);
        *batch_out = smaller_batch;
      }

      if (num_rows_ <= 0) {
        // We've run out of num_rows before batches
        ARROW_RETURN_NOT_OK(Close());
      }
    } else {
      // We've run out of batches before num_rows
      ARROW_RETURN_NOT_OK(Close());
    }

    return arrow::Status::OK();
  }

  arrow::Status Close() override {
    if (done_) {
      return arrow::Status::OK();
    } else {
      done_ = true;
      arrow::Status result = reader_->Close();
      return result;
    }
  }

 private:
  bool done_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::RecordBatchReader> reader_;
  int64_t num_rows_;
};

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatchReader> RecordBatchReader__Head(
    const std::shared_ptr<arrow::RecordBatchReader>& reader, int64_t num_rows) {
  if (num_rows <= 0) {
    // If we are never going to pull any batches from this reader, close it
    // immediately.
    StopIfNotOk(reader->Close());
    return ValueOrStop(arrow::RecordBatchReader::Make({}, reader->schema()));
  } else {
    return std::make_shared<RecordBatchReaderHead>(reader, num_rows);
  }
}

// -------- RecordBatchStreamReader

// [[arrow::export]]
std::shared_ptr<arrow::ipc::RecordBatchStreamReader> ipc___RecordBatchStreamReader__Open(
    const std::shared_ptr<arrow::io::InputStream>& stream) {
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.memory_pool = gc_memory_pool();
  return ValueOrStop(arrow::ipc::RecordBatchStreamReader::Open(stream, options));
}

// -------- RecordBatchFileReader

// [[arrow::export]]
std::shared_ptr<arrow::Schema> ipc___RecordBatchFileReader__schema(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  return reader->schema();
}

// [[arrow::export]]
int ipc___RecordBatchFileReader__num_record_batches(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  return reader->num_record_batches();
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> ipc___RecordBatchFileReader__ReadRecordBatch(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader, int i) {
  if (i < 0 && i >= reader->num_record_batches()) {
    cpp11::stop("Record batch index out of bounds");
  }
  return ValueOrStop(reader->ReadRecordBatch(i));
}

// [[arrow::export]]
std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc___RecordBatchFileReader__Open(
    const std::shared_ptr<arrow::io::RandomAccessFile>& file) {
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.memory_pool = gc_memory_pool();
  return ValueOrStop(arrow::ipc::RecordBatchFileReader::Open(file, options));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_RecordBatchFileReader(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  // RecordBatchStreamReader inherits from RecordBatchReader
  // but RecordBatchFileReader apparently does not
  int num_batches = reader->num_record_batches();
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches(num_batches);
  for (int i = 0; i < num_batches; i++) {
    batches[i] = ValueOrStop(reader->ReadRecordBatch(i));
  }

  return ValueOrStop(arrow::Table::FromRecordBatches(std::move(batches)));
}

// [[arrow::export]]
cpp11::list ipc___RecordBatchFileReader__batches(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>& reader) {
  auto n = reader->num_record_batches();
  std::vector<std::shared_ptr<arrow::RecordBatch>> res(n);

  for (int i = 0; i < n; i++) {
    res[i] = ValueOrStop(reader->ReadRecordBatch(i));
  }

  return arrow::r::to_r_list(res);
}
