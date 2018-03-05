#include "plasma/format/plasma.pb.h"

#include "arrow/util/logging.h"
#include "plasma/io.h"

using namespace google::protobuf;

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
    std::cout << "calling" << method->name() << std::endl;
    std::cout << "size = " << request->ByteSize() << std::endl;
    ARROW_CHECK_OK(plasma::WriteProto(fd_, method->index(), request));
    // int64_t size = request->ByteSize();
    // request->SerializeToFileDescriptor(fd_);
    // ARROW_CHECK_OK(plasma::WriteBytes(fd_, reinterpret_cast<uint8_t*>(&size), sizeof(size)));
    // request->SerializeToFileDescriptor(fd_);
  }
 private:
  int fd_;
};
