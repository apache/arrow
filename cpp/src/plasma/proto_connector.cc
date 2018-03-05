#include "plasma/proto_connector.h"

#include "plasma/fling.h"

using namespace plasma;

void myCallback() {
}

int main() {
  auto channel = std::unique_ptr<RpcChannelImpl>(new RpcChannelImpl("/tmp/plasma", 5, 100));
  auto service = std::unique_ptr<rpc::PlasmaStore_Stub>(new rpc::PlasmaStore_Stub(channel.get()));
  auto controller = std::unique_ptr<SocketRpcController>(new SocketRpcController());

  rpc::CreateRequest* request = new rpc::CreateRequest();
  request->set_object_id("hhhhhhhhhhhhhhhhhhhh");
  rpc::CreateReply* response = new rpc::CreateReply();

  service->Create(controller.get(), request, response, nullptr);

  std::cout << "response " << response->mmap_size() << std::endl;

  int fd = recv_fd(channel->conn());

  rpc::SealRequest* request2 = new rpc::SealRequest();
  request2->set_object_id("hhhhhhhhhhhhhhhhhhhh");
  rpc::SealReply* response2 = new rpc::SealReply();

  service->Seal(controller.get(), request2, response2, nullptr);
}
