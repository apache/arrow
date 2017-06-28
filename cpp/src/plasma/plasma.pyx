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

  cdef cppclass CUniqueID" UniqueID":

    @staticmethod
    CUniqueID from_binary(const c_string& binary)

    c_bool operator==(const CUniqueID& rhs) const

    c_string hex()

cdef extern from "plasma/plasma.h":
    cdef int64_t kDigestSize

cdef extern from "plasma/client.h" nogil:

  cdef cppclass CPlasmaClient" PlasmaClient":

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

  cdef struct CObjectBuffer" ObjectBuffer":
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

  cdef:
    ObjectID object_id
    PlasmaClient client

  def __cinit__(self, ObjectID object_id, PlasmaClient client):
    self.object_id = object_id
    self.client = client

  def __dealloc__(self):
    self.client.release(self.object_id)

cdef class PlasmaClient:

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
    check_status(self.client.get().Connect(store_socket_name.encode(), manager_socket_name.encode(), release_delay))

  def create(self, ObjectID object_id, data_size, c_string metadata=b""):
    cdef uint8_t* data
    check_status(self.client.get().Create(object_id.data, data_size, <uint8_t*>(metadata.data()), metadata.size(), &data))
    return self._make_plasma_buffer(object_id, data, data_size)

  def get(self, object_ids, timeout_ms=-1):
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
    cdef c_vector[CObjectBuffer] object_buffers
    self._get_object_buffers(object_ids, timeout_ms, &object_buffers)
    result = []
    for i in range(object_buffers.size()):
      result.append(self._make_plasma_buffer(object_ids[i], object_buffers[i].metadata, object_buffers[i].metadata_size))
    return result

  def seal(self, ObjectID object_id):
    check_status(self.client.get().Seal(object_id.data))

  def release(self, ObjectID object_id):
    check_status(self.client.get().Release(object_id.data))

  def contains(self, ObjectID object_id):
    cdef c_bool is_contained
    check_status(self.client.get().Contains(object_id.data, &is_contained))
    return is_contained

  def hash(self, ObjectID object_id):
    cdef c_vector[uint8_t] digest = c_vector[uint8_t](kDigestSize)
    check_status(self.client.get().Hash(object_id.data, digest.data()))
    return bytes(digest[:])

  def evict(self, int64_t num_bytes):
    cdef int64_t num_bytes_evicted
    check_status(self.client.get().Evict(num_bytes, num_bytes_evicted))
    return num_bytes_evicted

  def subscribe(self):
    check_status(self.client.get().Subscribe(&self.notification_fd))

  def get_next_notification(self):
    cdef ObjectID object_id = ObjectID(20 * b"\0")
    cdef int64_t data_size
    cdef int64_t metadata_size
    check_status(self.client.get().GetNotification(self.notification_fd, &object_id.data, &data_size, &metadata_size))
    return object_id, data_size, metadata_size

  def disconnect(self):
    check_status(self.client.get().Disconnect())
