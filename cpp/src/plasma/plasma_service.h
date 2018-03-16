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

#ifndef PLASMA_SERVICE_H
#define PLASMA_SERVICE_H

#include "plasma/format/plasma.pb.h"
#include "plasma/io.h"

using google::protobuf::Closure;
using google::protobuf::RpcController;

namespace plasma {

class PlasmaStore;
struct Client;
class PlasmaObject;

class PlasmaService : public rpc::PlasmaStore {
 public:
   PlasmaService(plasma::PlasmaStore* store) { store_ = store; }
   ~PlasmaService() {}

   Status ProcessMessage(Client* client);

   plasma::PlasmaIO* plasma_io();

   void Create(RpcController* controller,
               const rpc::CreateRequest* request,
               rpc::CreateReply* response,
               Closure* done) override;

   void Get(RpcController* controller,
            const rpc::GetRequest* request,
            rpc::GetReply* response,
            Closure* done) override;

   void Release(RpcController* controller,
                const rpc::ReleaseRequest* request,
                rpc::Void* response,
                Closure* done) override;
   void Contains(::google::protobuf::RpcController* controller,
                        const ::plasma::rpc::ContainsRequest* request,
                        ::plasma::rpc::ContainsReply* response,
                        ::google::protobuf::Closure* done) override {}

   void Seal(RpcController* controller,
             const rpc::SealRequest* request,
             rpc::Void* response,
             Closure* done) override;

   void Evict(::google::protobuf::RpcController* controller,
                        const ::plasma::rpc::EvictRequest* request,
                        ::plasma::rpc::EvictReply* response,
                        ::google::protobuf::Closure* done) override {}
   void Subscribe(::google::protobuf::RpcController* controller,
                        const ::plasma::rpc::SubscribeRequest* request,
                        ::plasma::rpc::Void* response,
                        ::google::protobuf::Closure* done) override {}
   void Connect(::google::protobuf::RpcController* controller,
                        const ::plasma::rpc::ConnectRequest* request,
                        ::plasma::rpc::ConnectReply* response,
                        ::google::protobuf::Closure* done) override {}
 private:
  plasma::PlasmaStore* store_;
  plasma::Client* client_;
  plasma::PlasmaIO plasma_io_;
  int fd_to_return_;
};

void ReadPlasmaObject(const rpc::PlasmaObjectSpec* spec, PlasmaObject* object);

void UpdateObjectSpec(const PlasmaObject& object, rpc::PlasmaObjectSpec* spec);

}  // namespace plasma

#endif  // PLASMA_SERVICE_H
