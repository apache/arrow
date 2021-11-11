.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. currentmodule:: pyarrow.cuda

CUDA Integration
================

Arrow is not limited to CPU buffers (located in the computer's main memory,
also named "host memory").  It also has provisions for accessing buffers
located on a CUDA-capable GPU device (in "device memory").

.. note::
   This functionality is optional and must have been enabled at build time.
   If this is not done by your package manager, you might have to build Arrow
   yourself.

CUDA Contexts
-------------

A CUDA context represents access to a particular CUDA-capable device.
For example, this is creating a CUDA context accessing CUDA device number 0::

   >>> from pyarrow import cuda
   >>> ctx = cuda.Context(0)
   >>>

CUDA Buffers
------------

A CUDA buffer can be created by copying data from host memory to the memory
of a CUDA device, using the :meth:`Context.buffer_from_data` method.
The source data can be any Python buffer-like object, including Arrow buffers::

   >>> import numpy as np
   >>> arr = np.arange(4, dtype=np.int32)
   >>> arr.nbytes
   16
   >>> cuda_buf = ctx.buffer_from_data(arr)
   >>> type(cuda_buf)
   pyarrow._cuda.CudaBuffer
   >>> cuda_buf.size     # The buffer's size in bytes
   16
   >>> cuda_buf.address  # The buffer's address in device memory
   30088364544
   >>> cuda_buf.context.device_number
   0

Conversely, you can copy back a CUDA buffer to device memory, getting a regular
CPU buffer::

   >>> buf = cuda_buf.copy_to_host()
   >>> type(buf)
   pyarrow.lib.Buffer
   >>> np.frombuffer(buf, dtype=np.int32)
   array([0, 1, 2, 3], dtype=int32)

.. warning::
   Many Arrow functions expect a CPU buffer but will not check the buffer's
   actual type.  You will get a crash if you pass a CUDA buffer to such a
   function::

      >>> pa.py_buffer(b"x" * 16).equals(cuda_buf)
      Segmentation fault

Numba Integration
-----------------

There is not much you can do directly with Arrow CUDA buffers from Python,
but they support interoperation with `Numba <https://numba.pydata.org/>`_,
a JIT compiler which can turn Python code into optimized CUDA kernels.

Arrow to Numba
~~~~~~~~~~~~~~

First let's define a Numba CUDA kernel operating on an ``int32`` array.  Here,
we will simply increment each array element (assuming the array is writable)::

   import numba.cuda

   @numba.cuda.jit
   def increment_by_one(an_array):
       pos = numba.cuda.grid(1)
       if pos < an_array.size:
           an_array[pos] += 1

Then we need to wrap our CUDA buffer into a Numba "device array" with the right
array metadata (shape, strides and datatype).  This is necessary so that Numba
can identify the array's characteristics and compile the kernel with the
appropriate type declarations.

In this case the metadata can simply be got from the original Numpy array.
Note the GPU data isn't copied, just pointed to::

   >>> from numba.cuda.cudadrv.devicearray import DeviceNDArray
   >>> device_arr = DeviceNDArray(arr.shape, arr.strides, arr.dtype, gpu_data=cuda_buf.to_numba())

(ideally we could have defined an Arrow array in CPU memory, copied it to CUDA
memory without losing type information, and then invoked the Numba kernel on it
without constructing the DeviceNDArray by hand; this is not yet possible)

Finally we can run the Numba CUDA kernel on the Numba device array (here
with a 16x16 grid size)::

   >>> increment_by_one[16, 16](device_arr)

And the results can be checked by copying back the CUDA buffer to CPU memory::

   >>> np.frombuffer(cuda_buf.copy_to_host(), dtype=np.int32)
   array([1, 2, 3, 4], dtype=int32)

Numba to Arrow
~~~~~~~~~~~~~~

Conversely, a Numba-created device array can be viewed as an Arrow CUDA buffer,
using the :meth:`CudaBuffer.from_numba` factory method.

For the sake of example, let's first create a Numba device array::

   >>> arr = np.arange(10, 14, dtype=np.int32)
   >>> arr
   array([10, 11, 12, 13], dtype=int32)
   >>> device_arr = numba.cuda.to_device(arr)

Then we can create a CUDA buffer pointing the device array's memory.
We don't need to pass a CUDA context explicitly this time: the appropriate
CUDA context is automatically retrieved and adapted from the Numba object.

::

   >>> cuda_buf = cuda.CudaBuffer.from_numba(device_arr.gpu_data)
   >>> cuda_buf.size
   16
   >>> cuda_buf.address
   30088364032
   >>> cuda_buf.context.device_number
   0

Of course, we can copy the CUDA buffer back to host memory::

   >>> np.frombuffer(cuda_buf.copy_to_host(), dtype=np.int32)
   array([10, 11, 12, 13], dtype=int32)

.. seealso::
   Documentation for Numba's `CUDA support <https://numba.pydata.org/numba-doc/latest/cuda/index.html>`_.
