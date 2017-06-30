# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from libcpp cimport bool as c_bool, nullptr
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libc.stdint cimport int64_t, uint8_t, uintptr_t

from pyarrow.lib cimport Buffer, NativeFile, check_status
from pyarrow.includes.libarrow cimport MutableBuffer, CBuffer, CFixedSizeBufferWrite, CStatus

cdef class FixedSizeBufferOutputStream(NativeFile):

    def __cinit__(self, Buffer buffer):
        self.wr_file.reset(new CFixedSizeBufferWrite(buffer.buffer))
        self.is_readable = 0
        self.is_writeable = 1
        self.is_open = True

cdef extern from "plasma/common.h" nogil:

  cdef cppclass CUniqueID" plasma::UniqueID":

    @staticmethod
    CUniqueID from_binary(const c_string& binary)

    c_bool operator==(const CUniqueID& rhs) const

    c_string hex()

cdef extern from "plasma/plasma.h":
    cdef int64_t kDigestSize" plasma::kDigestSize"

cdef extern from "plasma/client.h" nogil:

  cdef cppclass CPlasmaClient" plasma::PlasmaClient":

    CPlasmaClient()

    CStatus Connect(const c_string& store_socket_name, const c_string& manager_socket_name, int release_delay)

    CStatus Create(const CUniqueID& object_id, int64_t data_size, const uint8_t* metadata,
      int64_t metadata_size, uint8_t** data)

    CStatus Get(const CUniqueID* object_ids, int64_t num_objects, int64_t timeout_ms, CObjectBuffer* object_buffers)

    CStatus Seal(const CUniqueID& object_id)

    CStatus Evict(int64_t num_bytes, int64_t& num_bytes_evicted)

    CStatus Hash(const CUniqueID& object_id, uint8_t* digest)

    CStatus Release(const CUniqueID& object_id)

    CStatus Contains(const CUniqueID& object_id, c_bool* has_object)

    CStatus Subscribe(int* fd)

    CStatus GetNotification(int fd, CUniqueID* object_id, int64_t* data_size, int64_t* metadata_size)

    CStatus Disconnect()

cdef extern from "plasma/client.h" nogil:

  cdef struct CObjectBuffer" plasma::ObjectBuffer":
    int64_t data_size
    uint8_t* data
    int64_t metadata_size
    uint8_t* metadata

cdef class ObjectID:

  cdef:
    CUniqueID data

  def __cinit__(self, object_id):
    self.data = CUniqueID.from_binary(object_id)

  def __richcmp__(ObjectID self, ObjectID object_id, operation):
    assert operation == 2, "only equality implemented so far"
    return self.data == object_id.data

  def __repr__(self):
    return "ObjectID(" + self.data.hex().decode() + ")"

cdef class PlasmaBuffer(Buffer):
  """This is the type of objects returned by calls to get with a PlasmaClient.

  We define our own class instead of directly returning a buffer object so that
  we can add a custom destructor which notifies Plasma that the object is no
  longer being used, so the memory in the Plasma store backing the object can
  potentially be freed.

  Attributes:
    object_id (ObjectID): The ID of the object in the buffer.
    client (PlasmaClient): The PlasmaClient that we use to communicate
      with the store and manager.
  """

  cdef:
    ObjectID object_id
    PlasmaClient client

  def __cinit__(self, ObjectID object_id, PlasmaClient client):
    """Initialize a PlasmaBuffer."""
    self.object_id = object_id
    self.client = client

  def __dealloc__(self):
    """Notify Plasma that the object is no longer needed.

    If the plasma client has been shut down, then don't do anything.
    """
    self.client.release(self.object_id)

cdef class PlasmaClient:
  """The PlasmaClient is used to interface with a plasma store and manager.

  The PlasmaClient can ask the PlasmaStore to allocate a new buffer, seal a
  buffer, and get a buffer. Buffers are referred to by object IDs, which are
  strings.
  """

  cdef:
    shared_ptr[CPlasmaClient] client
    int notification_fd

  def __cinit__(self):
    self.client.reset(new CPlasmaClient())
    self.notification_fd = -1

  cdef _get_object_buffers(self, object_ids, int64_t timeout_ms, c_vector[CObjectBuffer]* result):
    cdef c_vector[CUniqueID] ids
    cdef ObjectID object_id
    for object_id in object_ids:
      ids.push_back(object_id.data)
    result[0].resize(ids.size())
    check_status(self.client.get().Get(ids.data(), ids.size(), timeout_ms, result[0].data()))

  cdef _make_plasma_buffer(self, ObjectID object_id, uint8_t* data, int64_t size):
    cdef shared_ptr[MutableBuffer] buffer
    buffer.reset(new MutableBuffer(data, size))
    result = PlasmaBuffer(object_id, self)
    result.init(<shared_ptr[CBuffer]>(buffer))
    return result

  def connect(self, store_socket_name, manager_socket_name, release_delay):
    """Conect the PlasmaClient to a plasma store and optionally a manager.

    Args:
      store_socket_name (str): Name of the socket the plasma store is listening
        at.
      manager_socket_name (str): Name of the socket the plasma manager is
        listening at.
      release_delay (int): The maximum number of objects that the client will
        keep and delay releasing (for caching reasons).
    """
    check_status(self.client.get().Connect(store_socket_name.encode(), manager_socket_name.encode(), release_delay))

  def create(self, ObjectID object_id, data_size, c_string metadata=b""):
    """Create a new buffer in the PlasmaStore for a particular object ID.

    The returned buffer is mutable until seal is called.

    Args:
      object_id (ObjectID): The object ID used to identify an object.
      size (int): The size in bytes of the created buffer.
      metadata (bytes): An optional string of bytes encoding whatever metadata
        the user wishes to encode.

    Raises:
      PlasmaObjectExists: This exception is raised if the object could
        not be created because there already is an object with the same ID in
        the plasma store.
      PlasmaStoreFull: This exception is raised if the object could
        not be created because the plasma store is unable to evict enough
        objects to create room for it.
    """
    cdef uint8_t* data
    check_status(self.client.get().Create(object_id.data, data_size, <uint8_t*>(metadata.data()), metadata.size(), &data))
    return self._make_plasma_buffer(object_id, data, data_size)

  def get(self, object_ids, timeout_ms=-1):
    """Returns data buffer from the PlasmaStore based on object ID.

    If the object has not been sealed yet, this call will block. The retrieved
    buffer is immutable.

    Args:
      object_ids (List[ObjectID]): A list of ObjectIDs used to identify some objects.
      timeout_ms (int): The number of milliseconds that the get call should
        block before timing out and returning. Pass -1 if the call should block
        and 0 if the call should return immediately.

    Returns:
      List of PlasmaBuffers for the data associated with the object_ids and None
      if the object was not available.
    """
    cdef c_vector[CObjectBuffer] object_buffers
    self._get_object_buffers(object_ids, timeout_ms, &object_buffers)
    result = []
    for i in range(object_buffers.size()):
      if object_buffers[i].data_size != -1:
        result.append(self._make_plasma_buffer(object_ids[i], object_buffers[i].data, object_buffers[i].data_size))
      else:
        result.append(None)
    return result

  def get_metadata(self, object_ids, timeout_ms=-1):
    """Returns metadata buffer from the PlasmaStore based on object ID.

    If the object has not been sealed yet, this call will block. The retrieved
    buffer is immutable.

    Args:
      object_ids (List[ObjectID]): A list of ObjectIDs used to identify some objects.
      timeout_ms (int): The number of milliseconds that the get call should
        block before timing out and returning. Pass -1 if the call should block
        and 0 if the call should return immediately.

    Returns:
      List of PlasmaBuffers for the metadata associated with the object_ids and None
      if the object was not available.
    """
    cdef c_vector[CObjectBuffer] object_buffers
    self._get_object_buffers(object_ids, timeout_ms, &object_buffers)
    result = []
    for i in range(object_buffers.size()):
      result.append(self._make_plasma_buffer(object_ids[i], object_buffers[i].metadata, object_buffers[i].metadata_size))
    return result

  def seal(self, ObjectID object_id):
    """Seal the buffer in the PlasmaStore for a particular object ID.

    Once a buffer has been sealed, the buffer is immutable and can only be
    accessed through get.

    Args:
      object_id (str): A string used to identify an object.
    """
    check_status(self.client.get().Seal(object_id.data))

  def release(self, ObjectID object_id):
    """Notify Plasma that the object is no longer needed."""
    check_status(self.client.get().Release(object_id.data))

  def contains(self, ObjectID object_id):
    """Check if the object is present and has been sealed in the PlasmaStore.

    Args:
      object_id (str): A string used to identify an object.
    """
    cdef c_bool is_contained
    check_status(self.client.get().Contains(object_id.data, &is_contained))
    return is_contained

  def hash(self, ObjectID object_id):
    """Compute the checksum of an object in the object store.

    Args:
      object_id (str): A string used to identify an object.

    Returns:
      A digest string object's hash. If the object isn't in the object
      store, the string will have length zero.
    """
    cdef c_vector[uint8_t] digest = c_vector[uint8_t](kDigestSize)
    check_status(self.client.get().Hash(object_id.data, digest.data()))
    return bytes(digest[:])

  def evict(self, int64_t num_bytes):
    """Evict some objects until to recover some bytes.

    Recover at least num_bytes bytes if possible.

    Args:
      num_bytes (int): The number of bytes to attempt to recover.
    """
    cdef int64_t num_bytes_evicted = -1
    check_status(self.client.get().Evict(num_bytes, num_bytes_evicted))
    return num_bytes_evicted

  def subscribe(self):
    """Subscribe to notifications about sealed objects."""
    check_status(self.client.get().Subscribe(&self.notification_fd))

  def get_next_notification(self):
    """Get the next notification from the notification socket."""
    cdef ObjectID object_id = ObjectID(20 * b"\0")
    cdef int64_t data_size
    cdef int64_t metadata_size
    check_status(self.client.get().GetNotification(self.notification_fd, &object_id.data, &data_size, &metadata_size))
    return object_id, data_size, metadata_size

  def disconnect(self):
    check_status(self.client.get().Disconnect())
