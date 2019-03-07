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

// [[Rcpp::export]]
int64_t ipc___Message__body_length(const std::unique_ptr<arrow::ipc::Message>& message) {
  return message->body_length();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Buffer> ipc___Message__metadata(
    const std::unique_ptr<arrow::ipc::Message>& message) {
  return message->metadata();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Buffer> ipc___Message__body(
    const std::unique_ptr<arrow::ipc::Message>& message) {
  return message->body();
}

// [[Rcpp::export]]
int64_t ipc___Message__Verify(const std::unique_ptr<arrow::ipc::Message>& message) {
  return message->Verify();
}

// [[Rcpp::export]]
arrow::ipc::Message::Type ipc___Message__type(
    const std::unique_ptr<arrow::ipc::Message>& message) {
  return message->type();
}

// [[Rcpp::export]]
bool ipc___Message__Equals(const std::unique_ptr<arrow::ipc::Message>& x,
                           const std::unique_ptr<arrow::ipc::Message>& y) {
  return x->Equals(*y);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> ipc___ReadRecordBatch__Message__Schema(
    const std::unique_ptr<arrow::ipc::Message>& message,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<arrow::RecordBatch> batch;
  STOP_IF_NOT_OK(arrow::ipc::ReadRecordBatch(*message, schema, &batch));
  return batch;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Schema> ipc___ReadSchema_InputStream(
    const std::shared_ptr<arrow::io::InputStream>& stream) {
  std::shared_ptr<arrow::Schema> schema;
  STOP_IF_NOT_OK(arrow::ipc::ReadSchema(stream.get(), &schema));
  return schema;
}

//--------- MessageReader

// [[Rcpp::export]]
std::unique_ptr<arrow::ipc::MessageReader> ipc___MessageReader__Open(
    const std::shared_ptr<arrow::io::InputStream>& stream) {
  return arrow::ipc::MessageReader::Open(stream);
}

// [[Rcpp::export]]
std::unique_ptr<arrow::ipc::Message> ipc___MessageReader__ReadNextMessage(
    const std::unique_ptr<arrow::ipc::MessageReader>& reader) {
  std::unique_ptr<arrow::ipc::Message> message;
  STOP_IF_NOT_OK(reader->ReadNextMessage(&message));
  return message;
}

// [[Rcpp::export]]
std::unique_ptr<arrow::ipc::Message> ipc___ReadMessage(
    const std::shared_ptr<arrow::io::InputStream>& stream) {
  std::unique_ptr<arrow::ipc::Message> message;
  STOP_IF_NOT_OK(arrow::ipc::ReadMessage(stream.get(), &message));
  return message;
}
