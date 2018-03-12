#include "plasma/format/plasma.pb.h"

#include "arrow/util/logging.h"
#include "plasma/plasma.h"
#include "plasma/io.h"

using namespace google::protobuf;

namespace plasma {

void ReadPlasmaObject(const rpc::PlasmaObjectSpec* spec, PlasmaObject* object) {
  object->store_fd = spec->segment_index();
  object->data_offset = spec->data_offset();
  object->data_size = spec->data_size();
  object->metadata_offset = spec->metadata_offset();
  object->metadata_size = spec->metadata_size();
  object->device_num = spec->device_num();
}

class SocketRpcController: public google::protobuf::RpcController {
public:
  SocketRpcController() {
    Reset();
  };

  virtual void Reset() {
    failed_ = false;
    error_ = "";
  }
  virtual bool Failed() const {
    return failed_;
  }
  virtual std::string ErrorText() const {
    return error_;
  }
  virtual void StartCancel() {
    assert(false);
  }
  virtual void SetFailed(const std::string& reason) {
    failed_ = true;
    error_ = reason;
  }
  virtual bool IsCanceled() const {
    assert(false);
  }
  virtual void NotifyOnCancel(google::protobuf::Closure* callback) {
    assert(false);
  }
 private:
  bool failed_;
  std::string error_;
};

class RpcChannelImpl : public RpcChannel {
 public:
  RpcChannelImpl(const std::string& pathname, int num_retries, int64_t timeout) {
    ARROW_CHECK_OK(plasma::ConnectIpcSocketRetry(pathname, num_retries, timeout, &fd_));
  }
  virtual void CallMethod(const MethodDescriptor* method,
                          RpcController* controller,
                          const Message* request,
                          Message* response,
                          Closure* done) {
    ARROW_CHECK_OK(plasma_io_.WriteProto(fd_, method->index(), request));
    if (response->GetDescriptor()->full_name() != "plasma.rpc.Void") {
      int64_t type;
      ARROW_CHECK_OK(plasma_io_.ReadProto(nullptr, fd_, &type, &response));
    }
  }
  int conn() { return fd_; }
 private:
  int fd_;
  PlasmaIO plasma_io_;
};

}  // namespace plasma
