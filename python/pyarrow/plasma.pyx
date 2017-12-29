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
from cpython.pycapsule cimport *

import collections
import pyarrow

from pyarrow.lib cimport Buffer, NativeFile, check_status
from pyarrow.includes.libarrow cimport (CBuffer, CMutableBuffer,
                                        CFixedSizeBufferWriter, CStatus)


PLASMA_WAIT_TIMEOUT = 2 ** 30


cdef extern from "plasma/common.h" nogil:

    cdef cppclass CUniqueID" plasma::UniqueID":

        @staticmethod
        CUniqueID from_binary(const c_string& binary)

        @staticmethod
        CUniqueID from_random()

        c_bool operator==(const CUniqueID& rhs) const

        c_string hex() const

        c_string binary() const

    cdef struct CObjectRequest" plasma::ObjectRequest":
        CUniqueID object_id
        int type
        int status


cdef extern from "plasma/common.h":
    cdef int64_t kDigestSize" plasma::kDigestSize"

    cdef enum ObjectRequestType:
        PLASMA_QUERY_LOCAL"plasma::PLASMA_QUERY_LOCAL",
        PLASMA_QUERY_ANYWHERE"plasma::PLASMA_QUERY_ANYWHERE"

    cdef int ObjectStatusLocal"plasma::ObjectStatusLocal"
    cdef int ObjectStatusRemote"plasma::ObjectStatusRemote"

cdef extern from "plasma/client.h" nogil:

    cdef cppclass CPlasmaClient" plasma::PlasmaClient":

        CPlasmaClient()

        CStatus Connect(const c_string& store_socket_name,
                        const c_string& manager_socket_name,
                        int release_delay, int num_retries)

        CStatus Create(const CUniqueID& object_id, int64_t data_size,
                       const uint8_t* metadata, int64_t metadata_size,
                       const shared_ptr[CBuffer]* data)

        CStatus Get(const CUniqueID* object_ids, int64_t num_objects,
                    int64_t timeout_ms, CObjectBuffer* object_buffers)

        CStatus Seal(const CUniqueID& object_id)

        CStatus Evict(int64_t num_bytes, int64_t& num_bytes_evicted)

        CStatus Hash(const CUniqueID& object_id, uint8_t* digest)

        CStatus Release(const CUniqueID& object_id)

        CStatus Contains(const CUniqueID& object_id, c_bool* has_object)

        CStatus Subscribe(int* fd)

        CStatus GetNotification(int fd, CUniqueID* object_id,
                                int64_t* data_size, int64_t* metadata_size)

        CStatus Disconnect()

        CStatus Fetch(int num_object_ids, const CUniqueID* object_ids)

        CStatus Wait(int64_t num_object_requests,
                     CObjectRequest* object_requests,
                     int num_ready_objects, int64_t timeout_ms,
                     int* num_objects_ready)

        CStatus Transfer(const char* addr, int port,
                         const CUniqueID& object_id)


cdef extern from "plasma/client.h" nogil:

    cdef struct CObjectBuffer" plasma::ObjectBuffer":
        int64_t data_size
        shared_ptr[CBuffer] data
        int64_t metadata_size
        shared_ptr[CBuffer] metadata


def make_object_id(object_id):
    return ObjectID(object_id)


cdef class ObjectID:
    """
    An ObjectID represents a string of bytes used to identify Plasma objects.
    """

    cdef:
        CUniqueID data

    def __cinit__(self, object_id):
        self.data = CUniqueID.from_binary(object_id)

    def __richcmp__(ObjectID self, ObjectID object_id, operation):
        if operation != 2:
            raise ValueError("operation != 2 (only equality is supported)")
        return self.data == object_id.data

    def __hash__(self):
        return hash(self.data.binary())

    def __repr__(self):
        return "ObjectID(" + self.data.hex().decode() + ")"

    def __reduce__(self):
        return (make_object_id, (self.data.binary(),))

    def binary(self):
        """
        Return the binary representation of this ObjectID.

        Returns
        -------
        bytes
            Binary representation of the ObjectID.
        """
        return self.data.binary()

    @staticmethod
    def from_random():
        cdef CUniqueID data = CUniqueID.from_random()
        return ObjectID(data.binary())


cdef class ObjectNotAvailable:
    """
    Placeholder for an object that was not available within the given timeout.
    """
    pass


cdef class PlasmaBuffer(Buffer):
    """
    This is the type returned by calls to get with a PlasmaClient.

    We define our own class instead of directly returning a buffer object so
    that we can add a custom destructor which notifies Plasma that the object
    is no longer being used, so the memory in the Plasma store backing the
    object can potentially be freed.

    Attributes
    ----------
    object_id : ObjectID
        The ID of the object in the buffer.
    client : PlasmaClient
        The PlasmaClient that we use to communicate with the store and manager.
    """

    cdef:
        ObjectID object_id
        PlasmaClient client

    def __cinit__(self, ObjectID object_id, PlasmaClient client):
        """
        Initialize a PlasmaBuffer.
        """
        self.object_id = object_id
        self.client = client

    def __dealloc__(self):
        """
        Notify Plasma that the object is no longer needed.

        If the plasma client has been shut down, then don't do anything.
        """
        self.client.release(self.object_id)


cdef class PlasmaClient:
    """
    The PlasmaClient is used to interface with a plasma store and manager.

    The PlasmaClient can ask the PlasmaStore to allocate a new buffer, seal a
    buffer, and get a buffer. Buffers are referred to by object IDs, which are
    strings.
    """

    cdef:
        shared_ptr[CPlasmaClient] client
        int notification_fd
        c_string store_socket_name
        c_string manager_socket_name

    def __cinit__(self):
        self.client.reset(new CPlasmaClient())
        self.notification_fd = -1
        self.store_socket_name = ""
        self.manager_socket_name = ""

    cdef _get_object_buffers(self, object_ids, int64_t timeout_ms,
                             c_vector[CObjectBuffer]* result):
        cdef c_vector[CUniqueID] ids
        cdef ObjectID object_id
        for object_id in object_ids:
            ids.push_back(object_id.data)
        result[0].resize(ids.size())
        with nogil:
            check_status(self.client.get().Get(ids.data(), ids.size(),
                         timeout_ms, result[0].data()))

    cdef _make_plasma_buffer(self, ObjectID object_id, shared_ptr[CBuffer] buffer,
                             int64_t size):
        result = PlasmaBuffer(object_id, self)
        result.init(buffer)
        return result

    cdef _make_mutable_plasma_buffer(self, ObjectID object_id, uint8_t* data,
                                     int64_t size):
        cdef shared_ptr[CBuffer] buffer
        buffer.reset(new CMutableBuffer(data, size))
        result = PlasmaBuffer(object_id, self)
        result.init(buffer)
        return result

    @property
    def store_socket_name(self):
        return self.store_socket_name.decode()

    @property
    def manager_socket_name(self):
        return self.manager_socket_name.decode()

    def create(self, ObjectID object_id, int64_t data_size,
               c_string metadata=b""):
        """
        Create a new buffer in the PlasmaStore for a particular object ID.

        The returned buffer is mutable until seal is called.

        Parameters
        ----------
        object_id : ObjectID
            The object ID used to identify an object.
        size : int
            The size in bytes of the created buffer.
        metadata : bytes
            An optional string of bytes encoding whatever metadata the user
            wishes to encode.

        Raises
        ------
        PlasmaObjectExists
            This exception is raised if the object could not be created because
            there already is an object with the same ID in the plasma store.

        PlasmaStoreFull: This exception is raised if the object could
                not be created because the plasma store is unable to evict
                enough objects to create room for it.
        """
        cdef shared_ptr[CBuffer] data
        with nogil:
            check_status(self.client.get().Create(object_id.data, data_size,
                                                  <uint8_t*>(metadata.data()),
                                                  metadata.size(), &data))
        return self._make_mutable_plasma_buffer(object_id, data.get().mutable_data(), data_size)

    def get_buffers(self, object_ids, timeout_ms=-1):
        """
        Returns data buffer from the PlasmaStore based on object ID.

        If the object has not been sealed yet, this call will block. The
        retrieved buffer is immutable.

        Parameters
        ----------
        object_ids : list
            A list of ObjectIDs used to identify some objects.
        timeout_ms : int
            The number of milliseconds that the get call should block before
            timing out and returning. Pass -1 if the call should block and 0
            if the call should return immediately.

        Returns
        -------
        list
            List of PlasmaBuffers for the data associated with the object_ids
            and None if the object was not available.
        """
        cdef c_vector[CObjectBuffer] object_buffers
        self._get_object_buffers(object_ids, timeout_ms, &object_buffers)
        result = []
        for i in range(object_buffers.size()):
            if object_buffers[i].data_size != -1:
                result.append(
                    self._make_plasma_buffer(object_ids[i],
                                             object_buffers[i].data,
                                             object_buffers[i].data_size))
            else:
                result.append(None)
        return result

    def get_metadata(self, object_ids, timeout_ms=-1):
        """
        Returns metadata buffer from the PlasmaStore based on object ID.

        If the object has not been sealed yet, this call will block. The
        retrieved buffer is immutable.

        Parameters
        ----------
        object_ids : list
            A list of ObjectIDs used to identify some objects.
        timeout_ms : int
            The number of milliseconds that the get call should block before
            timing out and returning. Pass -1 if the call should block and 0
            if the call should return immediately.

        Returns
        -------
        list
            List of PlasmaBuffers for the metadata associated with the
            object_ids and None if the object was not available.
        """
        cdef c_vector[CObjectBuffer] object_buffers
        self._get_object_buffers(object_ids, timeout_ms, &object_buffers)
        result = []
        for i in range(object_buffers.size()):
            result.append(
                self._make_plasma_buffer(object_ids[i],
                                         object_buffers[i].metadata,
                                         object_buffers[i].metadata_size))
        return result

    def put(self, object value, ObjectID object_id=None, int memcopy_threads=6,
            serialization_context=None):
        """
        Store a Python value into the object store.

        Parameters
        ----------
        value : object
            A Python object to store.
        object_id : ObjectID, default None
            If this is provided, the specified object ID will be used to refer
            to the object.
        memcopy_threads : int, default 6
            The number of threads to use to write the serialized object into
            the object store for large objects.
        serialization_context : pyarrow.SerializationContext, default None
            Custom serialization and deserialization context.

        Returns
        -------
        The object ID associated to the Python object.
        """
        cdef ObjectID target_id = (object_id if object_id
                                   else ObjectID.from_random())
        serialized = pyarrow.serialize(value, serialization_context)
        buffer = self.create(target_id, serialized.total_bytes)
        stream = pyarrow.FixedSizeBufferWriter(buffer)
        stream.set_memcopy_threads(memcopy_threads)
        serialized.write_to(stream)
        self.seal(target_id)
        return target_id

    def get(self, object_ids, int timeout_ms=-1, serialization_context=None):
        """
        Get one or more Python values from the object store.

        Parameters
        ----------
        object_ids : list or ObjectID
            Object ID or list of object IDs associated to the values we get
            from the store.
        timeout_ms : int, default -1
            The number of milliseconds that the get call should block before
            timing out and returning. Pass -1 if the call should block and 0
            if the call should return immediately.
        serialization_context : pyarrow.SerializationContext, default None
            Custom serialization and deserialization context.

        Returns
        -------
        list or object
            Python value or list of Python values for the data associated with
            the object_ids and ObjectNotAvailable if the object was not
            available.
        """
        if isinstance(object_ids, collections.Sequence):
            results = []
            buffers = self.get_buffers(object_ids, timeout_ms)
            for i in range(len(object_ids)):
                # buffers[i] is None if this object was not available within
                # the timeout
                if buffers[i]:
                    val = pyarrow.deserialize(buffers[i],
                                              serialization_context)
                    results.append(val)
                else:
                    results.append(ObjectNotAvailable)
            return results
        else:
            return self.get([object_ids], timeout_ms, serialization_context)[0]

    def seal(self, ObjectID object_id):
        """
        Seal the buffer in the PlasmaStore for a particular object ID.

        Once a buffer has been sealed, the buffer is immutable and can only be
        accessed through get.

        Parameters
        ----------
        object_id : ObjectID
            A string used to identify an object.
        """
        with nogil:
            check_status(self.client.get().Seal(object_id.data))

    def release(self, ObjectID object_id):
        """
        Notify Plasma that the object is no longer needed.

        Parameters
        ----------
        object_id : ObjectID
            A string used to identify an object.
        """
        with nogil:
            check_status(self.client.get().Release(object_id.data))

    def contains(self, ObjectID object_id):
        """
        Check if the object is present and sealed in the PlasmaStore.

        Parameters
        ----------
        object_id : ObjectID
            A string used to identify an object.
        """
        cdef c_bool is_contained
        with nogil:
            check_status(self.client.get().Contains(object_id.data,
                                                    &is_contained))
        return is_contained

    def hash(self, ObjectID object_id):
        """
        Compute the checksum of an object in the object store.

        Parameters
        ----------
        object_id : ObjectID
            A string used to identify an object.

        Returns
        -------
        bytes
            A digest string object's hash. If the object isn't in the object
            store, the string will have length zero.
        """
        cdef c_vector[uint8_t] digest = c_vector[uint8_t](kDigestSize)
        with nogil:
            check_status(self.client.get().Hash(object_id.data,
                                                digest.data()))
        return bytes(digest[:])

    def evict(self, int64_t num_bytes):
        """
        Evict some objects until to recover some bytes.

        Recover at least num_bytes bytes if possible.

        Parameters
        ----------
        num_bytes : int
            The number of bytes to attempt to recover.
        """
        cdef int64_t num_bytes_evicted = -1
        with nogil:
            check_status(self.client.get().Evict(num_bytes, num_bytes_evicted))
        return num_bytes_evicted

    def transfer(self, address, int port, ObjectID object_id):
        """
        Transfer local object with id object_id to another plasma instance

        Parameters
        ----------
        addr : str
            IPv4 address of the plasma instance the object is sent to.
        port : int
            Port number of the plasma instance the object is sent to.
        object_id : str
            A string used to identify an object.
        """
        cdef c_string addr = address.encode()
        with nogil:
            check_status(self.client.get()
                         .Transfer(addr.c_str(), port, object_id.data))

    def fetch(self, object_ids):
        """
        Fetch the objects with the given IDs from other plasma managers.

        Parameters
        ----------
        object_ids : list
            A list of strings used to identify the objects.
        """
        cdef c_vector[CUniqueID] ids
        cdef ObjectID object_id
        for object_id in object_ids:
            ids.push_back(object_id.data)
        with nogil:
            check_status(self.client.get().Fetch(ids.size(), ids.data()))

    def wait(self, object_ids, int64_t timeout=PLASMA_WAIT_TIMEOUT,
             int num_returns=1):
        """
        Wait until num_returns objects in object_ids are ready.
        Currently, the object ID arguments to wait must be unique.

        Parameters
        ----------
        object_ids : list
            List of object IDs to wait for.
        timeout :int
            Return to the caller after timeout milliseconds.
        num_returns : int
            We are waiting for this number of objects to be ready.

        Returns
        -------
        list
            List of object IDs that are ready.
        list
            List of object IDs we might still wait on.
        """
        # Check that the object ID arguments are unique. The plasma manager
        # currently crashes if given duplicate object IDs.
        if len(object_ids) != len(set(object_ids)):
            raise Exception("Wait requires a list of unique object IDs.")
        cdef int64_t num_object_requests = len(object_ids)
        cdef c_vector[CObjectRequest] object_requests = (
            c_vector[CObjectRequest](num_object_requests))
        cdef int num_objects_ready = 0
        cdef ObjectID object_id
        for i, object_id in enumerate(object_ids):
            object_requests[i].object_id = object_id.data
            object_requests[i].type = PLASMA_QUERY_ANYWHERE
        with nogil:
            check_status(self.client.get().Wait(num_object_requests,
                                                object_requests.data(),
                                                num_returns, timeout,
                                                &num_objects_ready))
        cdef int num_to_return = min(num_objects_ready, num_returns)
        ready_ids = []
        waiting_ids = set(object_ids)
        cdef int num_returned = 0
        for i in range(len(object_ids)):
            if num_returned == num_to_return:
                break
            if (object_requests[i].status == ObjectStatusLocal or
                    object_requests[i].status == ObjectStatusRemote):
                ready_ids.append(
                    ObjectID(object_requests[i].object_id.binary()))
                waiting_ids.discard(
                    ObjectID(object_requests[i].object_id.binary()))
                num_returned += 1
        return ready_ids, list(waiting_ids)

    def subscribe(self):
        """Subscribe to notifications about sealed objects."""
        with nogil:
            check_status(self.client.get().Subscribe(&self.notification_fd))

    def get_next_notification(self):
        """
        Get the next notification from the notification socket.

        Returns
        -------
        ObjectID
            The object ID of the object that was stored.
        int
            The data size of the object that was stored.
        int
            The metadata size of the object that was stored.
        """
        cdef ObjectID object_id = ObjectID(20 * b"\0")
        cdef int64_t data_size
        cdef int64_t metadata_size
        with nogil:
            check_status(self.client.get()
                         .GetNotification(self.notification_fd,
                                          &object_id.data,
                                          &data_size,
                                          &metadata_size))
        return object_id, data_size, metadata_size

    def to_capsule(self):
        return PyCapsule_New(<void *>self.client.get(), "plasma", NULL)

    def disconnect(self):
        """
        Disconnect this client from the Plasma store.
        """
        with nogil:
            check_status(self.client.get().Disconnect())


def connect(store_socket_name, manager_socket_name, int release_delay,
            int num_retries=-1):
    """
    Return a new PlasmaClient that is connected a plasma store and
    optionally a manager.

    Parameters
    ----------
    store_socket_name : str
        Name of the socket the plasma store is listening at.
    manager_socket_name : str
        Name of the socket the plasma manager is listening at.
    release_delay : int
        The maximum number of objects that the client will keep and
        delay releasing (for caching reasons).
    num_retries : int, default -1
        Number of times to try to connect to plasma store. Default value of -1
        uses the default (50)
    """
    cdef PlasmaClient result = PlasmaClient()
    result.store_socket_name = store_socket_name.encode()
    result.manager_socket_name = manager_socket_name.encode()
    with nogil:
        check_status(result.client.get()
                     .Connect(result.store_socket_name,
                              result.manager_socket_name,
                              release_delay, num_retries))
    return result
