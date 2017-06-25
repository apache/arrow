# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from libcpp cimport bool as c_bool, nullptr
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared
from libcpp.string cimport string as c_string
from libc.stdint cimport int64_t, uint8_t

from pyarrow.lib cimport Buffer, NativeFile
from pyarrow.includes.libarrow cimport CBuffer, CFixedSizeBufferWrite

cdef extern from "arrow/api.h" namespace "arrow" nogil:
    # We can later add more of the common status factory methods as needed
    cdef CStatus CStatus_OK "Status::OK"()

    cdef cppclass CStatus "arrow::Status":
        CStatus()

        c_string ToString()

        c_bool ok()
        c_bool IsIOError()
        c_bool IsOutOfMemory()
        c_bool IsInvalid()
        c_bool IsKeyError()
        c_bool IsNotImplemented()
        c_bool IsTypeError()



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

    c_string hex()

cdef extern from "plasma/client.h" nogil:

  cdef cppclass CPlasmaClient" PlasmaClient":

    CPlasmaClient()

    CStatus Connect(const c_string& store_socket_name, const c_string& manager_socket_name, int release_delay)

    CStatus Create(const CUniqueID& object_id, int64_t data_size, uint8_t* metadata,
      int64_t metadata_size, uint8_t** data)

    CStatus Seal(const CUniqueID& object_id)

    CStatus Disconnect()

cdef class ObjectID:

  cdef:
    CUniqueID data

  def __cinit__(self, object_id):
    self.data = CUniqueID.from_binary(object_id)

  def __repr__(self):
    return "ObjectID(" + self.data.hex().decode() + ")"

cdef class PlasmaClient:

  cdef:
    shared_ptr[CPlasmaClient] client

  def __cinit__(self):
    self.client.reset(new CPlasmaClient())

  def connect(self, store_socket_name, manager_socket_name, release_delay):
    self.client.get().Connect(store_socket_name, manager_socket_name, release_delay)

  def create(self, ObjectID object_id, data_size):
    cdef uint8_t* data
    cdef shared_ptr[CBuffer] buffer
    self.client.get().Create(object_id.data, data_size, NULL, 0, &data)
    buffer.reset(new CBuffer(data, data_size))
    result = Buffer()
    result.init(buffer)
    return result

  def seal(self, ObjectID object_id):
    self.client.get().Seal(object_id.data)

  def disconnect(self):
    self.client.get().Disconnect()
