#include "plasma/proto_connector.h"

using namespace plasma;

void myCallback() {
}

int main() {
  auto channel = std::unique_ptr<RpcChannel>(new RpcChannelImpl("/tmp/plasma", 5, 100));
  auto service = std::unique_ptr<rpc::PlasmaStore_Stub>(new rpc::PlasmaStore_Stub(channel.get()));
  auto controller = std::unique_ptr<SocketRpcController>(new SocketRpcController());

  rpc::CreateRequest* request = new rpc::CreateRequest();
  request->set_object_id("hello");
  rpc::CreateReply* response = new rpc::CreateReply();

  service->Create(controller.get(), request, response, NewCallback(myCallback));

  rpc::SealRequest* request2 = new rpc::SealRequest();
  rpc::SealReply* response2 = new rpc::SealReply();

  service->Seal(controller.get(), request2, response2, NewCallback(myCallback));
}
