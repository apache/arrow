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

#include "arrow/flight/transport/grpc/async_grpc_service_internal.h"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace arrow::flight::transport::grpc::detail {

namespace {

/**
 * A generic streaming reactor for handling async gRPC calls in the Flight service.
 * One request message is read, followed by N response messages, then the stream finishes.
 */
class StreamingReactor : public AsyncReactorBase {
 public:
  StreamingReactor(AsyncCallContext flight_context, AsyncGenericFlightServerBase* base)
      : AsyncReactorBase(std::move(flight_context)), base_(base) {
    StartRead(&request_buf_);
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      FinishOnce(MakeFlightError(FlightStatusCode::Internal, "Failed to read request"));
      return;
    }
    Start().AddCallback([this, token = hold()](const arrow::Status& status) {
      if (!finished()) {
        if (!status.ok()) {
          FinishOnce(status);
        } else {
          WriteNextMessage();
        }
      }
    });
  }

  void OnWriteDone(bool ok) override {
    if (finished()) {
      return;
    }
    if (!ok) {
      FinishOnce(MakeFlightError(FlightStatusCode::Internal, "Failed to write response"));
      return;
    }
    WriteNextMessage();
  }

 protected:
  /// Run the handler. Its continuation publishes the state NextMessage() reads
  /// before the first NextMessage(), and every later NextMessage() follows the
  /// previous write's completion: that state is never shared concurrently, so
  /// it needs no lock.
  virtual arrow::Future<> Start() = 0;
  virtual arrow::Future<bool> NextMessage() = 0;

  void WriteNextMessage() {
    if (finished()) {
      return;
    }
    NextMessage().AddCallback(
        [this, token = hold()](const arrow::Result<bool>& has_next) {
          if (!finished()) {
            if (!has_next.ok()) {
              FinishOnce(has_next.status());
            } else if (!*has_next) {
              FinishOnce(arrow::Status::OK());
            } else {
              WriteIfLive([this] { StartWrite(&write_buf_); });
            }
          }
        });
  }

  AsyncGenericFlightServerBase* base_;
  ::grpc::ByteBuffer request_buf_;
  ::grpc::ByteBuffer write_buf_;
};

class ListActionsReactor final : public StreamingReactor {
 public:
  using StreamingReactor::StreamingReactor;

 protected:
  arrow::Future<> Start() override {
    return base_->ListActionsAsync(flight_context())
        .Then([this](const std::vector<ActionType>& actions) -> arrow::Status {
          actions_ = actions;
          next_ = 0;
          return arrow::Status::OK();
        });
  }

  arrow::Future<bool> NextMessage() override {
    if (next_ >= actions_.size()) {
      return arrow::Future<bool>::MakeFinished(false);
    }
    pb::ActionType pb_type;
    const auto status = internal::ToProto(actions_[next_++], &pb_type);
    if (!status.ok()) {
      return arrow::Future<bool>::MakeFinished(status);
    }
    write_buf_ = MakeWriteBuffer(pb_type);
    return arrow::Future<bool>::MakeFinished(true);
  }

 private:
  std::vector<ActionType> actions_;
  size_t next_ = 0;
};

class DoActionReactor final : public StreamingReactor {
 public:
  using StreamingReactor::StreamingReactor;

 protected:
  arrow::Future<> Start() override {
    auto action = ParseProtoRequest<pb::Action, Action>(request_buf_, "Action");
    if (!action.ok()) {
      return arrow::Future<>::MakeFinished(action.status());
    }
    return base_->DoActionAsync(flight_context(), *action)
        .Then([this](const std::shared_ptr<AsyncResultStream>& stream) -> arrow::Status {
          if (stream == nullptr) {
            return arrow::Status::Cancelled();
          }
          stream_ = stream;
          return arrow::Status::OK();
        });
  }

  arrow::Future<bool> NextMessage() override {
    if (stream_ == nullptr) {
      return arrow::Future<bool>::MakeFinished(false);
    }
    return stream_->NextAsync().Then(
        [this](const std::shared_ptr<Result>& result) -> arrow::Result<bool> {
          if (result == nullptr) {
            return false;
          }
          pb::Result pb_result;
          ARROW_RETURN_NOT_OK(internal::ToProto(*result, &pb_result));
          write_buf_ = MakeWriteBuffer(pb_result);
          return true;
        });
  }

 private:
  std::shared_ptr<AsyncResultStream> stream_;
};

class ListFlightsReactor final : public StreamingReactor {
 public:
  using StreamingReactor::StreamingReactor;

 protected:
  arrow::Future<> Start() override {
    auto criteria = ParseProtoRequest<pb::Criteria, Criteria>(request_buf_, "Criteria");
    if (!criteria.ok()) {
      return arrow::Future<>::MakeFinished(criteria.status());
    }
    criteria_ = *criteria;
    return base_->ListFlightsAsync(flight_context(), &criteria_)
        .Then(
            [this](const std::shared_ptr<AsyncFlightListing>& listing) -> arrow::Status {
              listing_ = listing;
              return arrow::Status::OK();
            });
  }

  arrow::Future<bool> NextMessage() override {
    if (listing_ == nullptr) {
      return arrow::Future<bool>::MakeFinished(false);
    }
    return listing_->NextAsync().Then(
        [this](const std::shared_ptr<FlightInfo>& info) -> arrow::Result<bool> {
          if (info == nullptr) {
            return false;
          }
          pb::FlightInfo pb_info;
          ARROW_RETURN_NOT_OK(internal::ToProto(*info, &pb_info));
          write_buf_ = MakeWriteBuffer(pb_info);
          return true;
        });
  }

 private:
  Criteria criteria_;
  std::shared_ptr<AsyncFlightListing> listing_;
};

}  // namespace

::grpc::ServerGenericBidiReactor* MakeListActionsReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base) {
  return new ListActionsReactor(std::move(flight_context), base);
}

::grpc::ServerGenericBidiReactor* MakeDoActionReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base) {
  return new DoActionReactor(std::move(flight_context), base);
}

::grpc::ServerGenericBidiReactor* MakeListFlightsReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base) {
  return new ListFlightsReactor(std::move(flight_context), base);
}

}  // namespace arrow::flight::transport::grpc::detail
