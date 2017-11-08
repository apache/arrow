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

#include "plasma/protocol.h"

#include "flatbuffers/flatbuffers.h"
#include "plasma/plasma_generated.h"

#include "plasma/common.h"
#include "plasma/io.h"

namespace plasma {

using flatbuffers::uoffset_t;

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
to_flatbuffer(flatbuffers::FlatBufferBuilder* fbb, const ObjectID* object_ids,
              int64_t num_objects) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (int64_t i = 0; i < num_objects; i++) {
    results.push_back(fbb->CreateString(object_ids[i].binary()));
  }
  return fbb->CreateVector(results);
}

Status PlasmaReceive(int sock, int64_t message_type, std::vector<uint8_t>* buffer) {
  int64_t type;
  RETURN_NOT_OK(ReadMessage(sock, &type, buffer));
  ARROW_CHECK(type == message_type)
      << "type = " << type << ", message_type = " << message_type;
  return Status::OK();
}

template <typename Message>
Status PlasmaSend(int sock, int64_t message_type, flatbuffers::FlatBufferBuilder* fbb,
                  const Message& message) {
  fbb->Finish(message);
  return WriteMessage(sock, message_type, fbb->GetSize(), fbb->GetBufferPointer());
}

// Create messages.

Status SendCreateRequest(int sock, ObjectID object_id, int64_t data_size,
                         int64_t metadata_size) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaCreateRequest(fbb, fbb.CreateString(object_id.binary()),
                                           data_size, metadata_size);
  return PlasmaSend(sock, MessageType_PlasmaCreateRequest, &fbb, message);
}

Status ReadCreateRequest(uint8_t* data, size_t size, ObjectID* object_id,
                         int64_t* data_size, int64_t* metadata_size) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaCreateRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *data_size = message->data_size();
  *metadata_size = message->metadata_size();
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

Status SendCreateReply(int sock, ObjectID object_id, PlasmaObject* object,
                       int error_code) {
  flatbuffers::FlatBufferBuilder fbb;
  PlasmaObjectSpec plasma_object(object->handle.store_fd, object->handle.mmap_size,
                                 object->data_offset, object->data_size,
                                 object->metadata_offset, object->metadata_size);
  auto message =
      CreatePlasmaCreateReply(fbb, fbb.CreateString(object_id.binary()), &plasma_object,
                              static_cast<PlasmaError>(error_code));
  return PlasmaSend(sock, MessageType_PlasmaCreateReply, &fbb, message);
}

Status ReadCreateReply(uint8_t* data, size_t size, ObjectID* object_id,
                       PlasmaObject* object) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaCreateReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  object->handle.store_fd = message->plasma_object()->segment_index();
  object->handle.mmap_size = message->plasma_object()->mmap_size();
  object->data_offset = message->plasma_object()->data_offset();
  object->data_size = message->plasma_object()->data_size();
  object->metadata_offset = message->plasma_object()->metadata_offset();
  object->metadata_size = message->plasma_object()->metadata_size();
  return plasma_error_status(message->error());
}

Status SendAbortRequest(int sock, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaAbortRequest(fbb, fbb.CreateString(object_id.binary()));
  return PlasmaSend(sock, MessageType_PlasmaAbortRequest, &fbb, message);
}

Status ReadAbortRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaAbortRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

Status SendAbortReply(int sock, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaAbortReply(fbb, fbb.CreateString(object_id.binary()));
  return PlasmaSend(sock, MessageType_PlasmaAbortReply, &fbb, message);
}

Status ReadAbortReply(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaAbortReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

// Seal messages.

Status SendSealRequest(int sock, ObjectID object_id, unsigned char* digest) {
  flatbuffers::FlatBufferBuilder fbb;
  auto digest_string = fbb.CreateString(reinterpret_cast<char*>(digest), kDigestSize);
  auto message =
      CreatePlasmaSealRequest(fbb, fbb.CreateString(object_id.binary()), digest_string);
  return PlasmaSend(sock, MessageType_PlasmaSealRequest, &fbb, message);
}

Status ReadSealRequest(uint8_t* data, size_t size, ObjectID* object_id,
                       unsigned char* digest) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaSealRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  ARROW_CHECK(message->digest()->size() == kDigestSize);
  memcpy(digest, message->digest()->data(), kDigestSize);
  return Status::OK();
}

Status SendSealReply(int sock, ObjectID object_id, int error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaSealReply(fbb, fbb.CreateString(object_id.binary()),
                                       static_cast<PlasmaError>(error));
  return PlasmaSend(sock, MessageType_PlasmaSealReply, &fbb, message);
}

Status ReadSealReply(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaSealReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return plasma_error_status(message->error());
}

// Release messages.

Status SendReleaseRequest(int sock, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaReleaseRequest(fbb, fbb.CreateString(object_id.binary()));
  return PlasmaSend(sock, MessageType_PlasmaReleaseRequest, &fbb, message);
}

Status ReadReleaseRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaReleaseRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

Status SendReleaseReply(int sock, ObjectID object_id, int error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaReleaseReply(fbb, fbb.CreateString(object_id.binary()),
                                          static_cast<PlasmaError>(error));
  return PlasmaSend(sock, MessageType_PlasmaReleaseReply, &fbb, message);
}

Status ReadReleaseReply(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaReleaseReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return plasma_error_status(message->error());
}

// Delete messages.

Status SendDeleteRequest(int sock, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaDeleteRequest(fbb, fbb.CreateString(object_id.binary()));
  return PlasmaSend(sock, MessageType_PlasmaDeleteRequest, &fbb, message);
}

Status ReadDeleteRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaReleaseReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

Status SendDeleteReply(int sock, ObjectID object_id, int error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaDeleteReply(fbb, fbb.CreateString(object_id.binary()),
                                         static_cast<PlasmaError>(error));
  return PlasmaSend(sock, MessageType_PlasmaDeleteReply, &fbb, message);
}

Status ReadDeleteReply(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaDeleteReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return plasma_error_status(message->error());
}

// Satus messages.

Status SendStatusRequest(int sock, const ObjectID* object_ids, int64_t num_objects) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      CreatePlasmaStatusRequest(fbb, to_flatbuffer(&fbb, object_ids, num_objects));
  return PlasmaSend(sock, MessageType_PlasmaStatusRequest, &fbb, message);
}

Status ReadStatusRequest(uint8_t* data, size_t size, ObjectID object_ids[],
                         int64_t num_objects) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaStatusRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  for (uoffset_t i = 0; i < num_objects; ++i) {
    object_ids[i] = ObjectID::from_binary(message->object_ids()->Get(i)->str());
  }
  return Status::OK();
}

Status SendStatusReply(int sock, ObjectID object_ids[], int object_status[],
                       int64_t num_objects) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      CreatePlasmaStatusReply(fbb, to_flatbuffer(&fbb, object_ids, num_objects),
                              fbb.CreateVector(object_status, num_objects));
  return PlasmaSend(sock, MessageType_PlasmaStatusReply, &fbb, message);
}

int64_t ReadStatusReply_num_objects(uint8_t* data, size_t size) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaStatusReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  return message->object_ids()->size();
}

Status ReadStatusReply(uint8_t* data, size_t size, ObjectID object_ids[],
                       int object_status[], int64_t num_objects) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaStatusReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  for (uoffset_t i = 0; i < num_objects; ++i) {
    object_ids[i] = ObjectID::from_binary(message->object_ids()->Get(i)->str());
  }
  for (uoffset_t i = 0; i < num_objects; ++i) {
    object_status[i] = message->status()->data()[i];
  }
  return Status::OK();
}

// Contains messages.

Status SendContainsRequest(int sock, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaContainsRequest(fbb, fbb.CreateString(object_id.binary()));
  return PlasmaSend(sock, MessageType_PlasmaContainsRequest, &fbb, message);
}

Status ReadContainsRequest(uint8_t* data, size_t size, ObjectID* object_id) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaContainsRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  return Status::OK();
}

Status SendContainsReply(int sock, ObjectID object_id, bool has_object) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      CreatePlasmaContainsReply(fbb, fbb.CreateString(object_id.binary()), has_object);
  return PlasmaSend(sock, MessageType_PlasmaContainsReply, &fbb, message);
}

Status ReadContainsReply(uint8_t* data, size_t size, ObjectID* object_id,
                         bool* has_object) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaContainsReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  *has_object = message->has_object();
  return Status::OK();
}

// Connect messages.

Status SendConnectRequest(int sock) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaConnectRequest(fbb);
  return PlasmaSend(sock, MessageType_PlasmaConnectRequest, &fbb, message);
}

Status ReadConnectRequest(uint8_t* data) { return Status::OK(); }

Status SendConnectReply(int sock, int64_t memory_capacity) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaConnectReply(fbb, memory_capacity);
  return PlasmaSend(sock, MessageType_PlasmaConnectReply, &fbb, message);
}

Status ReadConnectReply(uint8_t* data, size_t size, int64_t* memory_capacity) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaConnectReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *memory_capacity = message->memory_capacity();
  return Status::OK();
}

// Evict messages.

Status SendEvictRequest(int sock, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaEvictRequest(fbb, num_bytes);
  return PlasmaSend(sock, MessageType_PlasmaEvictRequest, &fbb, message);
}

Status ReadEvictRequest(uint8_t* data, size_t size, int64_t* num_bytes) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaEvictRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *num_bytes = message->num_bytes();
  return Status::OK();
}

Status SendEvictReply(int sock, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaEvictReply(fbb, num_bytes);
  return PlasmaSend(sock, MessageType_PlasmaEvictReply, &fbb, message);
}

Status ReadEvictReply(uint8_t* data, size_t size, int64_t& num_bytes) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaEvictReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  num_bytes = message->num_bytes();
  return Status::OK();
}

// Get messages.

Status SendGetRequest(int sock, const ObjectID* object_ids, int64_t num_objects,
                      int64_t timeout_ms) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaGetRequest(fbb, to_flatbuffer(&fbb, object_ids, num_objects),
                                        timeout_ms);
  return PlasmaSend(sock, MessageType_PlasmaGetRequest, &fbb, message);
}

Status ReadGetRequest(uint8_t* data, size_t size, std::vector<ObjectID>& object_ids,
                      int64_t* timeout_ms) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaGetRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  for (uoffset_t i = 0; i < message->object_ids()->size(); ++i) {
    auto object_id = message->object_ids()->Get(i)->str();
    object_ids.push_back(ObjectID::from_binary(object_id));
  }
  *timeout_ms = message->timeout_ms();
  return Status::OK();
}

Status SendGetReply(
    int sock, ObjectID object_ids[],
    std::unordered_map<ObjectID, PlasmaObject, UniqueIDHasher>& plasma_objects,
    int64_t num_objects) {
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<PlasmaObjectSpec> objects;

  for (int i = 0; i < num_objects; ++i) {
    const PlasmaObject& object = plasma_objects[object_ids[i]];
    objects.push_back(PlasmaObjectSpec(object.handle.store_fd, object.handle.mmap_size,
                                       object.data_offset, object.data_size,
                                       object.metadata_offset, object.metadata_size));
  }
  auto message =
      CreatePlasmaGetReply(fbb, to_flatbuffer(&fbb, object_ids, num_objects),
                           fbb.CreateVectorOfStructs(objects.data(), num_objects));
  return PlasmaSend(sock, MessageType_PlasmaGetReply, &fbb, message);
}

Status ReadGetReply(uint8_t* data, size_t size, ObjectID object_ids[],
                    PlasmaObject plasma_objects[], int64_t num_objects) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaGetReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  for (uoffset_t i = 0; i < num_objects; ++i) {
    object_ids[i] = ObjectID::from_binary(message->object_ids()->Get(i)->str());
  }
  for (uoffset_t i = 0; i < num_objects; ++i) {
    const PlasmaObjectSpec* object = message->plasma_objects()->Get(i);
    plasma_objects[i].handle.store_fd = object->segment_index();
    plasma_objects[i].handle.mmap_size = object->mmap_size();
    plasma_objects[i].data_offset = object->data_offset();
    plasma_objects[i].data_size = object->data_size();
    plasma_objects[i].metadata_offset = object->metadata_offset();
    plasma_objects[i].metadata_size = object->metadata_size();
  }
  return Status::OK();
}

// Fetch messages.

Status SendFetchRequest(int sock, const ObjectID* object_ids, int64_t num_objects) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      CreatePlasmaFetchRequest(fbb, to_flatbuffer(&fbb, object_ids, num_objects));
  return PlasmaSend(sock, MessageType_PlasmaFetchRequest, &fbb, message);
}

Status ReadFetchRequest(uint8_t* data, size_t size, std::vector<ObjectID>& object_ids) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaFetchRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  for (uoffset_t i = 0; i < message->object_ids()->size(); ++i) {
    object_ids.push_back(ObjectID::from_binary(message->object_ids()->Get(i)->str()));
  }
  return Status::OK();
}

// Wait messages.

Status SendWaitRequest(int sock, ObjectRequest object_requests[], int64_t num_requests,
                       int num_ready_objects, int64_t timeout_ms) {
  flatbuffers::FlatBufferBuilder fbb;

  std::vector<flatbuffers::Offset<ObjectRequestSpec>> object_request_specs;
  for (int i = 0; i < num_requests; i++) {
    object_request_specs.push_back(CreateObjectRequestSpec(
        fbb, fbb.CreateString(object_requests[i].object_id.binary()),
        object_requests[i].type));
  }

  auto message = CreatePlasmaWaitRequest(fbb, fbb.CreateVector(object_request_specs),
                                         num_ready_objects, timeout_ms);
  return PlasmaSend(sock, MessageType_PlasmaWaitRequest, &fbb, message);
}

Status ReadWaitRequest(uint8_t* data, size_t size, ObjectRequestMap& object_requests,
                       int64_t* timeout_ms, int* num_ready_objects) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaWaitRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *num_ready_objects = message->num_ready_objects();
  *timeout_ms = message->timeout();

  for (uoffset_t i = 0; i < message->object_requests()->size(); i++) {
    ObjectID object_id =
        ObjectID::from_binary(message->object_requests()->Get(i)->object_id()->str());
    ObjectRequest object_request({object_id, message->object_requests()->Get(i)->type(),
                                  ObjectStatus_Nonexistent});
    object_requests[object_id] = object_request;
  }
  return Status::OK();
}

Status SendWaitReply(int sock, const ObjectRequestMap& object_requests,
                     int num_ready_objects) {
  flatbuffers::FlatBufferBuilder fbb;

  std::vector<flatbuffers::Offset<ObjectReply>> object_replies;
  for (const auto& entry : object_requests) {
    const auto& object_request = entry.second;
    object_replies.push_back(CreateObjectReply(
        fbb, fbb.CreateString(object_request.object_id.binary()), object_request.status));
  }

  auto message = CreatePlasmaWaitReply(
      fbb, fbb.CreateVector(object_replies.data(), num_ready_objects), num_ready_objects);
  return PlasmaSend(sock, MessageType_PlasmaWaitReply, &fbb, message);
}

Status ReadWaitReply(uint8_t* data, size_t size, ObjectRequest object_requests[],
                     int* num_ready_objects) {
  DCHECK(data);

  auto message = flatbuffers::GetRoot<PlasmaWaitReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *num_ready_objects = message->num_ready_objects();
  for (int i = 0; i < *num_ready_objects; i++) {
    object_requests[i].object_id =
        ObjectID::from_binary(message->object_requests()->Get(i)->object_id()->str());
    object_requests[i].status = message->object_requests()->Get(i)->status();
  }
  return Status::OK();
}

// Subscribe messages.

Status SendSubscribeRequest(int sock) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaSubscribeRequest(fbb);
  return PlasmaSend(sock, MessageType_PlasmaSubscribeRequest, &fbb, message);
}

// Data messages.

Status SendDataRequest(int sock, ObjectID object_id, const char* address, int port) {
  flatbuffers::FlatBufferBuilder fbb;
  auto addr = fbb.CreateString(address, strlen(address));
  auto message =
      CreatePlasmaDataRequest(fbb, fbb.CreateString(object_id.binary()), addr, port);
  return PlasmaSend(sock, MessageType_PlasmaDataRequest, &fbb, message);
}

Status ReadDataRequest(uint8_t* data, size_t size, ObjectID* object_id, char** address,
                       int* port) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaDataRequest>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  DCHECK(message->object_id()->size() == sizeof(ObjectID));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  *address = strdup(message->address()->c_str());
  *port = message->port();
  return Status::OK();
}

Status SendDataReply(int sock, ObjectID object_id, int64_t object_size,
                     int64_t metadata_size) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = CreatePlasmaDataReply(fbb, fbb.CreateString(object_id.binary()),
                                       object_size, metadata_size);
  return PlasmaSend(sock, MessageType_PlasmaDataReply, &fbb, message);
}

Status ReadDataReply(uint8_t* data, size_t size, ObjectID* object_id,
                     int64_t* object_size, int64_t* metadata_size) {
  DCHECK(data);
  auto message = flatbuffers::GetRoot<PlasmaDataReply>(data);
  DCHECK(verify_flatbuffer(message, data, size));
  *object_id = ObjectID::from_binary(message->object_id()->str());
  *object_size = static_cast<int64_t>(message->object_size());
  *metadata_size = static_cast<int64_t>(message->metadata_size());
  return Status::OK();
}

}  // namespace plasma
