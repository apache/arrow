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
#include <mutex>
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
      if (!status.ok()) {
        FinishOnce(status);
        return;
      }
      WriteNextMessage();
    });
  }

  void OnWriteDone(bool ok) override {
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

  /// Drop the reference to the application's iterator when the RPC ends.
  ///
  /// An iterator that keeps its own pending NextAsync() future holds a
  /// continuation on this reactor, which holds the iterator: without this, that
  /// cycle survives the cancellation and neither is ever freed. It runs on the
  /// cancellation thread while NextMessage() may be inside the iterator on a
  /// callback thread, so both take stream_mutex_ and NextMessage() calls
  /// through a copy: the iterator outlives the call made on it.
  virtual void ForgetStream() {}

  void TearDownOnCancel() override {
    FinishOnce(arrow::Status::Cancelled());
    ForgetStream();
  }

  void WriteNextMessage() {
    if (finished()) {
      return;
    }
    NextMessage().AddCallback(
        [this, token = hold()](const arrow::Result<bool>& has_next) {
          if (!has_next.ok()) {
            FinishOnce(has_next.status());
          } else if (!*has_next) {
            FinishOnce(arrow::Status::OK());
          } else {
            WriteIfLive([this] { StartWrite(&write_buf_); });
          }
        });
  }

  AsyncGenericFlightServerBase* base_;
  /// Guards the application's iterator, see ForgetStream().
  std::mutex stream_mutex_;
};

class ListActionsReactor final : public StreamingReactor {
 public:
  using StreamingReactor::StreamingReactor;

 protected:
  arrow::Future<> Start() override {
    return base_->ListActionsAsync(flight_context())
        .Then([this](const std::vector<ActionType>& actions) -> arrow::Status {
          actions_ = actions;
          return arrow::Status::OK();
        });
  }

  arrow::Future<bool> NextMessage() override {
    if (next_ >= actions_.size()) {
      return false;
    }
    pb::ActionType pb_type;
    ARROW_RETURN_NOT_OK(internal::ToProto(actions_[next_++], &pb_type));
    write_buf_ = MakeWriteBuffer(pb_type);
    return true;
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
    ARROW_ASSIGN_OR_RAISE(
        auto action, (ParseProtoRequest<pb::Action, Action>(request_buf_, "Action")));
    return base_->DoActionAsync(flight_context(), action)
        .Then([this](const std::shared_ptr<AsyncResultStream>& stream) -> arrow::Status {
          if (stream == nullptr) {
            return arrow::Status::Cancelled();
          }
          std::lock_guard<std::mutex> lock(stream_mutex_);
          stream_ = stream;
          return arrow::Status::OK();
        });
  }

  arrow::Future<bool> NextMessage() override {
    std::shared_ptr<AsyncResultStream> stream;
    {
      std::lock_guard<std::mutex> lock(stream_mutex_);
      stream = stream_;
    }
    if (stream == nullptr) {
      // Forgotten: the RPC was cancelled.
      return false;
    }
    return stream->NextAsync().Then(
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

  void ForgetStream() override {
    std::lock_guard<std::mutex> lock(stream_mutex_);
    stream_.reset();
  }

 private:
  std::shared_ptr<AsyncResultStream> stream_;
};

class ListFlightsReactor final : public StreamingReactor {
 public:
  using StreamingReactor::StreamingReactor;

 protected:
  arrow::Future<> Start() override {
    ARROW_ASSIGN_OR_RAISE(
        criteria_, (ParseProtoRequest<pb::Criteria, Criteria>(request_buf_, "Criteria")));
    return base_->ListFlightsAsync(flight_context(), &criteria_)
        .Then(
            [this](const std::shared_ptr<AsyncFlightListing>& listing) -> arrow::Status {
              std::lock_guard<std::mutex> lock(stream_mutex_);
              listing_ = listing;
              return arrow::Status::OK();
            });
  }

  arrow::Future<bool> NextMessage() override {
    std::shared_ptr<AsyncFlightListing> listing;
    {
      std::lock_guard<std::mutex> lock(stream_mutex_);
      listing = listing_;
    }
    if (listing == nullptr) {
      return false;
    }
    return listing->NextAsync().Then(
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

  void ForgetStream() override {
    std::lock_guard<std::mutex> lock(stream_mutex_);
    listing_.reset();
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
