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

#ifndef PLASMA_STORE_H
#define PLASMA_STORE_H

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "plasma/common.h"
#include "plasma/eviction_policy.h"
#include "plasma/external_store.h"
#include "plasma/io/connection.h"
#include "plasma/plasma.h"

namespace arrow {
class Status;
}  // namespace arrow

namespace plasma {

namespace flatbuf {
enum class PlasmaError;
}  // namespace flatbuf

using flatbuf::PlasmaError;
using io::ClientConnection;

struct GetRequest;

class PlasmaStore {
 public:
  PlasmaStore(asio::io_context& main_context, std::string directory,
              bool hugepages_enabled, const std::string& stream_name,
              std::shared_ptr<ExternalStore> external_store);

  ~PlasmaStore();

  /// Get a const pointer to the internal PlasmaStoreInfo object.
  const PlasmaStoreInfo* GetPlasmaStoreInfo();

  /// Create a new object. The client must do a call to release_object to tell
  /// the store when it is done with the object.
  ///
  /// \param object_id Object ID of the object to be created.
  /// \param data_size Size in bytes of the object to be created.
  /// \param metadata_size Size in bytes of the object metadata.
  /// \param device_num The number of the device where the object is being
  ///        created.
  ///        device_num = 0 corresponds to the host,
  ///        device_num = 1 corresponds to GPU0,
  ///        device_num = 2 corresponds to GPU1, etc.
  /// \param client The client that created the object.
  /// \param result The object that has been created.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was created successfully.
  ///  - PlasmaError::ObjectExists, if an object with this ID is already
  ///    present in the store. In this case, the client should not call
  ///    plasma_release.
  ///  - PlasmaError::OutOfMemory, if the store is out of memory and
  ///    cannot create the object. In this case, the client should not call
  ///    plasma_release.
  PlasmaError CreateObject(const ObjectID& object_id, int64_t data_size,
                           int64_t metadata_size, int device_num,
                           const std::shared_ptr<ClientConnection>& client,
                           PlasmaObject* result);

  /// Abort a created but unsealed object. If the client is not the
  /// creator, then the abort will fail.
  ///
  /// \param object_id Object ID of the object to be aborted.
  /// \param client The client who created the object. If this does not
  ///   match the creator of the object, then the abort will fail.
  /// \return 1 if the abort succeeds, else 0.
  int AbortObject(const ObjectID& object_id,
                  const std::shared_ptr<ClientConnection>& client);

  /// Delete an specific object by object_id that have been created in the hash table.
  ///
  /// \param object_id Object ID of the object to be deleted.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was delete successfully.
  ///  - PlasmaError::ObjectNonexistent, if ths object isn't existed.
  ///  - PlasmaError::ObjectInUse, if the object is in use.
  PlasmaError DeleteObject(ObjectID& object_id);

  /// Evict objects returned by the eviction policy.
  ///
  /// \param object_ids Object IDs of the objects to be evicted.
  void EvictObjects(const std::vector<ObjectID>& object_ids);

  /// Process a get request from a client. This method assumes that we will
  /// eventually have these objects sealed. If one of the objects has not yet
  /// been sealed, the client that requested the object will be notified when it
  /// is sealed.
  ///
  /// For each object, the client must do a call to release_object to tell the
  /// store when it is done with the object.
  ///
  /// \param client The client making this request.
  /// \param object_ids Object IDs of the objects to be gotten.
  /// \param timeout_ms The timeout for the get request in milliseconds.
  Status ProcessGetRequest(const std::shared_ptr<ClientConnection>& client,
                           const std::vector<ObjectID>& object_ids, int64_t timeout_ms);

  /// Seal an object. The object is now immutable and can be accessed with get.
  ///
  /// \param object_id Object ID of the object to be sealed.
  /// \param digest The digest of the object. This is used to tell if two
  /// objects with the same object ID are the same.
  void SealObject(const ObjectID& object_id, unsigned char digest[]);

  /// Check if the plasma store contains an object:
  ///
  /// \param object_id Object ID that will be checked.
  /// \return OBJECT_FOUND if the object is in the store, OBJECT_NOT_FOUND if
  /// not
  ObjectStatus ContainsObject(const ObjectID& object_id);

  /// Record the fact that a particular client is no longer using an object.
  ///
  /// \param object_id The object ID of the object that is being released.
  /// \param client The client making this request.
  void ReleaseObject(const ObjectID& object_id,
                     const std::shared_ptr<ClientConnection>& client);

  /// Subscribe a file descriptor to updates about new sealed objects.
  ///
  /// \param client The client making this request.
  void SubscribeToUpdates(const std::shared_ptr<ClientConnection>& client);

 private:
  // Inform all subscribers that a new object has been sealed.
  void PushObjectReadyNotification(const ObjectID& object_id,
                                   const ObjectTableEntry& entry);

  // Inform all subscribers that an object has evicted.
  void PushObjectDeletionNotification(const ObjectID& object_id);

  void AddToClientObjectIds(const ObjectID& object_id, ObjectTableEntry* entry,
                            const std::shared_ptr<ClientConnection>& client);

  /// Remove a GetRequest and clean up the relevant data structures.
  ///
  /// \param get_request The GetRequest to remove.
  void RemoveGetRequest(GetRequest* get_request);

  /// Remove all of the GetRequests for a given client.
  ///
  /// \param client The client whose GetRequests should be removed.
  void RemoveGetRequestsForClient(const std::shared_ptr<ClientConnection>& client);

  /// Release all resources used by the client.
  ///
  /// \param client The client whose resources should be released.
  void ReleaseClientResources(const std::shared_ptr<ClientConnection>& client);

  void ReturnFromGet(GetRequest* get_req);

  void UpdateObjectGetRequests(const ObjectID& object_id);

  void EraseFromObjectTable(const ObjectID& object_id);

  uint8_t* AllocateMemory(size_t size, int* fd, int64_t* map_size, ptrdiff_t* offset);

  void IncreaseObjectRefCount(const ObjectID& object_id, ObjectTableEntry* entry);

  void DecreaseObjectRefCount(const ObjectID& object_id, ObjectTableEntry* entry);

#ifdef PLASMA_CUDA
  Status AllocateCudaMemory(int device_num, int64_t size, uint8_t** out_pointer,
                            std::shared_ptr<CudaIpcMemHandle>* out_ipc_handle);

  Status FreeCudaMemory(int device_num, int64_t size, uint8_t* out_pointer);
#endif

  /// Accept a client connection.
  void DoAccept();
  /// Handle an accepted client connection.
  void HandleAccept(const error_code& error);

  Status ProcessClientMessage(const std::shared_ptr<ClientConnection>& client,
                              int64_t message_type, int64_t message_size,
                              const uint8_t* message_data);

  /// Disconnect a client from the PlasmaStore.
  ///
  /// \param client The client that is disconnected.
  void ProcessDisconnectClient(const std::shared_ptr<ClientConnection>& client);

  /// The plasma store information, including the object tables, that is exposed
  /// to the eviction policy.
  PlasmaStoreInfo store_info_;
  /// The state that is managed by the eviction policy.
  EvictionPolicy eviction_policy_;
  /// A hash table mapping object IDs to a vector of the get requests that are
  /// waiting for the object to arrive.
  std::unordered_map<ObjectID, std::vector<GetRequest*>> object_get_requests_;

  std::unordered_set<std::shared_ptr<ClientConnection>> notification_clients_;

  std::unordered_set<std::shared_ptr<ClientConnection>> connected_clients_;

  std::unordered_set<ObjectID> deletion_cache_;

  /// Manages worker threads for handling asynchronous/multi-threaded requests
  /// for reading/writing data to/from external store.
  std::shared_ptr<ExternalStore> external_store_;
#ifdef PLASMA_CUDA
  arrow::cuda::CudaDeviceManager* manager_;
#endif
  asio::io_context& io_context_;
  /// The name of the stream this store server listens on.
  std::string stream_name_;
  /// An acceptor for new clients.
  io::PlasmaAcceptor acceptor_;
  /// The stream to listen on for new clients.
  io::PlasmaStream stream_;
};

}  // namespace plasma

#endif  // PLASMA_STORE_H
