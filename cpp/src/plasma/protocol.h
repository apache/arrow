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

#ifndef PLASMA_PROTOCOL_H
#define PLASMA_PROTOCOL_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/status.h"
#include "plasma/io/connection.h"
#include "plasma/plasma.h"
#include "plasma/plasma_generated.h"

namespace plasma {

using arrow::Status;
using flatbuf::PlasmaError;
using io::ClientConnection;
using io::ServerConnection;

template <class T>
bool VerifyFlatbuffer(T* object, const uint8_t* data, size_t size) {
  flatbuffers::Verifier verifier(data, size);
  return object->Verify(verifier);
}

/* Plasma Create message functions. */

Status SendCreateRequest(const std::shared_ptr<ServerConnection>& client,
                         ObjectID object_id, int64_t data_size, int64_t metadata_size,
                         int device_num);

Status ReadCreateRequest(const uint8_t* data, size_t size, ObjectID* object_id,
                         int64_t* data_size, int64_t* metadata_size, int* device_num);

Status SendCreateReply(const std::shared_ptr<ClientConnection>& client,
                       ObjectID object_id, PlasmaObject* object, PlasmaError error,
                       int64_t mmap_size);

Status ReadCreateReply(const uint8_t* data, size_t size, ObjectID* object_id,
                       PlasmaObject* object, int* store_fd, int64_t* mmap_size);

Status SendCreateAndSealRequest(const std::shared_ptr<ServerConnection>& client,
                                const ObjectID& object_id, const std::string& data,
                                const std::string& metadata, unsigned char* digest);

Status ReadCreateAndSealRequest(const uint8_t* data, size_t size, ObjectID* object_id,
                                std::string* object_data, std::string* metadata,
                                unsigned char* digest);

Status SendCreateAndSealReply(const std::shared_ptr<ClientConnection>& client,
                              PlasmaError error);

Status ReadCreateAndSealReply(const uint8_t* data, size_t size);

Status SendAbortRequest(const std::shared_ptr<ServerConnection>& client,
                        ObjectID object_id);

Status ReadAbortRequest(const uint8_t* data, size_t size, ObjectID* object_id);

Status SendAbortReply(const std::shared_ptr<ClientConnection>& client,
                      ObjectID object_id);

Status ReadAbortReply(const uint8_t* data, size_t size, ObjectID* object_id);

/* Plasma Seal message functions. */

Status SendSealRequest(const std::shared_ptr<ServerConnection>& client,
                       ObjectID object_id, unsigned char* digest);

Status ReadSealRequest(const uint8_t* data, size_t size, ObjectID* object_id,
                       unsigned char* digest);

Status SendSealReply(const std::shared_ptr<ClientConnection>& client, ObjectID object_id,
                     PlasmaError error);

Status ReadSealReply(const uint8_t* data, size_t size, ObjectID* object_id);

/* Plasma Get message functions. */

Status SendGetRequest(const std::shared_ptr<ServerConnection>& client,
                      const ObjectID* object_ids, int64_t num_objects,
                      int64_t timeout_ms);

Status ReadGetRequest(const uint8_t* data, size_t size, std::vector<ObjectID>& object_ids,
                      int64_t* timeout_ms);

Status SendGetReply(const std::shared_ptr<ClientConnection>& client,
                    ObjectID object_ids[],
                    std::unordered_map<ObjectID, PlasmaObject>& plasma_objects,
                    int64_t num_objects, const std::vector<int>& store_fds,
                    const std::vector<int64_t>& mmap_sizes);

Status ReadGetReply(const uint8_t* data, size_t size, ObjectID object_ids[],
                    PlasmaObject plasma_objects[], int64_t num_objects,
                    std::vector<int>& store_fds, std::vector<int64_t>& mmap_sizes);

/* Plasma Release message functions. */

Status SendReleaseRequest(const std::shared_ptr<ServerConnection>& client,
                          ObjectID object_id);

Status ReadReleaseRequest(const uint8_t* data, size_t size, ObjectID* object_id);

Status SendReleaseReply(const std::shared_ptr<ClientConnection>& client,
                        ObjectID object_id, PlasmaError error);

Status ReadReleaseReply(const uint8_t* data, size_t size, ObjectID* object_id);

/* Plasma Delete objects message functions. */

Status SendDeleteRequest(const std::shared_ptr<ServerConnection>& client,
                         const std::vector<ObjectID>& object_ids);

Status ReadDeleteRequest(const uint8_t* data, size_t size,
                         std::vector<ObjectID>* object_ids);

Status SendDeleteReply(const std::shared_ptr<ClientConnection>& client,
                       const std::vector<ObjectID>& object_ids,
                       const std::vector<PlasmaError>& errors);

Status ReadDeleteReply(const uint8_t* data, size_t size,
                       std::vector<ObjectID>* object_ids,
                       std::vector<PlasmaError>* errors);

/* Plasma Constains message functions. */

Status SendContainsRequest(const std::shared_ptr<ServerConnection>& client,
                           ObjectID object_id);

Status ReadContainsRequest(const uint8_t* data, size_t size, ObjectID* object_id);

Status SendContainsReply(const std::shared_ptr<ClientConnection>& client,
                         ObjectID object_id, bool has_object);

Status ReadContainsReply(const uint8_t* data, size_t size, ObjectID* object_id,
                         bool* has_object);

/* Plasma List message functions. */

Status SendListRequest(const std::shared_ptr<ServerConnection>& client);

Status ReadListRequest(const uint8_t* data, size_t size);

Status SendListReply(const std::shared_ptr<ClientConnection>& client,
                     const ObjectTable& objects);

Status ReadListReply(const uint8_t* data, size_t size, ObjectTable* objects);

/* Plasma Connect message functions. */

Status SendConnectRequest(const std::shared_ptr<ServerConnection>& client);

Status ReadConnectRequest(const uint8_t* data, size_t size);

Status SendConnectReply(const std::shared_ptr<ClientConnection>& client,
                        int64_t memory_capacity);

Status ReadConnectReply(const uint8_t* data, size_t size, int64_t* memory_capacity);

/* Plasma Evict message functions (no reply so far). */

Status SendEvictRequest(const std::shared_ptr<ServerConnection>& client,
                        int64_t num_bytes);

Status ReadEvictRequest(const uint8_t* data, size_t size, int64_t* num_bytes);

Status SendEvictReply(const std::shared_ptr<ClientConnection>& client, int64_t num_bytes);

Status ReadEvictReply(const uint8_t* data, size_t size, int64_t& num_bytes);

/* Data messages. */

Status SendDataRequest(const std::shared_ptr<ServerConnection>& client,
                       ObjectID object_id, const char* address, int port);

Status ReadDataRequest(const uint8_t* data, size_t size, ObjectID* object_id,
                       char** address, int* port);

Status SendDataReply(const std::shared_ptr<ClientConnection>& client, ObjectID object_id,
                     int64_t object_size, int64_t metadata_size);

Status ReadDataReply(const uint8_t* data, size_t size, ObjectID* object_id,
                     int64_t* object_size, int64_t* metadata_size);

/* Plasma notification message functions. */

Status SendSubscribeRequest(const std::shared_ptr<ServerConnection>& client);

void SerializeObjectDeletionNotification(const ObjectID& object_id,
                                         std::vector<uint8_t>* serialized);

void SerializeObjectSealedNotification(const ObjectID& object_id,
                                       const ObjectTableEntry& entry,
                                       std::vector<uint8_t>* serialized);
}  // namespace plasma

#endif /* PLASMA_PROTOCOL */
