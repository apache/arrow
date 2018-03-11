#include "plasma_service.h"

#include "plasma/fling.h"
#include "plasma/io.h"
#include "plasma/malloc.h"

namespace plasma {

Status PlasmaService::ProcessMessage(Client* client) {
  // Pass this on to the the next method executed on the PlasmaService.
  client_ = client;

  int64_t type = 0;
  google::protobuf::Message* request = nullptr;
  ARROW_CHECK_OK(plasma_io_.ReadProto(this, client->fd, &type, &request));

  const google::protobuf::ServiceDescriptor* service_descriptor = GetDescriptor();
  const google::protobuf::MethodDescriptor* method_descriptor = service_descriptor->method(type);

  google::protobuf::Message* response = GetResponsePrototype(method_descriptor).New();

  fd_to_return_ = -1;
  CallMethod(method_descriptor, nullptr, request, response, nullptr);

  HANDLE_SIGPIPE(plasma_io_.WriteProto(client->fd, type, response), client->fd);

  if (fd_to_return_ != -1) {
    //if (error_code == PlasmaError_OK && device_num == 0) {
    warn_if_sigpipe(send_fd(client->fd, fd_to_return_), client->fd);
    //}
  }
}

void UpdateObjectSpec(const PlasmaObject& object, rpc::PlasmaObjectSpec* spec) {
  spec->set_segment_index(object.store_fd);
  spec->set_data_offset(object.data_offset);
  spec->set_data_size(object.data_size);
  spec->set_metadata_offset(object.metadata_offset);
  spec->set_metadata_size(object.metadata_size);
  spec->set_device_num(object.device_num);
}

void PlasmaService::Create(RpcController* controller,
                           const rpc::CreateRequest* request,
                           rpc::CreateReply* response,
                           Closure* done) {
  PlasmaObject object;
  ObjectID object_id = ObjectID::from_binary(request->object_id());
  int error_code =
      store_->create_object(object_id, request->data_size(), request->metadata_size(),
                            request->device_num(), client_, &object);
  int64_t mmap_size = 0;
  if (error_code == PlasmaError_OK && request->device_num() == 0) {
    mmap_size = get_mmap_size(object.store_fd);
    fd_to_return_ = object.store_fd;
  }
  response->set_object_id(request->object_id());
  UpdateObjectSpec(object, response->mutable_plasma_object());
  response->set_error(static_cast<rpc::PlasmaError>(error_code));
  response->set_store_fd(object.store_fd);
  response->set_mmap_size(mmap_size);
}

void PlasmaService::Seal(RpcController* controller,
                         const rpc::SealRequest* request,
                         rpc::SealReply* response,
                         Closure* done) {
  ObjectID object_id = ObjectID::from_binary(request->object_id());
  store_->seal_object(object_id, request->digest().data());
}

void PlasmaService::Release(RpcController* controller,
             const rpc::ReleaseRequest* request,
             rpc::ReleaseReply* response,
             Closure* done) {
  ObjectID object_id = ObjectID::from_binary(request->object_id());
  store_->release_object(object_id, client_);
}

}  // namespace plasma
