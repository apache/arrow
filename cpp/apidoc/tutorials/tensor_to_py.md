<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Use Plasma to Access Tensors from C++ in Python
==============================================

This short tutorial shows how to use Arrow and the Plasma Store to send data
from C++ to Python.

In detail, we will show how to:
1. Serialize a floating-point array in C++ into an Arrow tensor
2. Save the Arrow tensor to Plasma
3. Access the Tensor in a Python process

This approach has the advantage that multiple python processes can all read
the tensor with zero-copy. Therefore, only one copy is necessary when we send
a tensor from one C++ process to many python processes.


Step 0: Set up
------
We will include the following header files and construct a Plasma client.

```cpp
#include <plasma/client.h>
#include <arrow/tensor.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

PlasmaClient client_;
ARROW_CHECK_OK(client_.Connect("/tmp/plasma", "", 0));
```


Step 1: Serialize a floating point array in C++ into an Arrow Tensor
--------------------------------------------------------------------
In this step, we will construct a floating-point array in C++.

```cpp
// Generate an Object ID for Plasma
ObjectID object_id = ObjectID::from_binary("11111111111111111111");

// Generate Float Array
int64_t input_length = 1000;
std::vector<float> input(input_length);
for (int64_t i = 0; i < input_length; ++i) {
  input[i] = 2.0;
}

// Create Arrow Tensor Object, no copy made!
// {input_length} is the shape of the tensor
auto value_buffer = Buffer::Wrap<float>(input);
Tensor t(float32(), value_buffer, {input_length});
```

Step 2: Save the Arrow Tensor to Plasma In-Memory Object Store
--------------------------------------------------------------
Continuing from Step 1, this step will save the tensor to Plasma Store. We
use `arrow::ipc::WriteTensor` to write the data.

The variable `meta_len` will contain the length of the tensor metadata
after the call to `arrow::ipc::WriteTensor`.

```cpp
// Get the size of the tensor to be stored in Plasma
int64_t datasize;
ARROW_CHECK_OK(ipc::GetTensorSize(t, &datasize));
int32_t meta_len = 0;

// Create the Plasma Object
// Plasma is responsible for initializing and resizing the buffer
// This buffer will contain the _serialized_ tensor
std::shared_ptr<Buffer> buffer;
ARROW_CHECK_OK(
    client_.Create(object_id, datasize, NULL, 0, &buffer));

// Writing Process, this will copy the tensor into Plasma
io::FixedSizeBufferWriter stream(buffer);
ARROW_CHECK_OK(arrow::ipc::WriteTensor(t, &stream, &meta_len, &datasize));

// Seal Plasma Object
// This computes a hash of the object data by default
ARROW_CHECK_OK(client_.Seal(object_id));
```

Step 3: Access the Tensor in a Python Process
---------------------------------------------
In Python, we will construct a Plasma client and point it to the store's socket.
The `inputs` variable will be a list of Object IDs in their raw byte string form.

```python
import pyarrow as pa
import pyarrow.plasma as plasma

plasma_client = plasma.connect('/tmp/plasma')

# inputs: a list of object ids
inputs = [20 * b'1']

# Construct Object ID and perform a batch get
object_ids = [plasma.ObjectID(inp) for inp in inputs]
buffers = plasma_client.get_buffers(object_ids)

# Read the tensor and convert to numpy array for each object
arrs = []
for buffer in buffers:
    reader = pa.BufferReader(buffer)
    t = pa.read_tensor(reader)
    arr = t.to_numpy()
    arrs.append(arr)

# arrs is now a list of numpy arrays
assert np.all(arrs[0] == 2.0 * np.ones(1000, dtype="float32"))
```
