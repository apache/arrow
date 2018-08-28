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

Use Plasma to Access Tensor from C++ in Python
==============================================

This short tutorial shows how to use Arrow and Plasma Store to send data 
from C++ to Python. 

In detail, we will show how to:
1. Serialize a floating-point array in C++ into Arrow Tensor
2. Save the Arrow Tensor to Plasma In-Memory Object Store
3. Access the Tensor in a Python Process

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


Step 1: Serialize a floating point array in C++ into Arrow Tensor
---------------------------------------------
In this step, we will construct a floating-point array in C++. `generate_input`
function takes the input length and returns the pointer to a `float` array 
filled with random elements. 

```cpp
// Generate Random Object ID for Plasma, 20 bytes
ObjectID object_id = ObjectID::from_random();

// Generate Float Array
int64_t input_length = 1000;
float* input = generate_input(input_length);

// Cast float array to bytes array
const uint8_t* bytes_array = reinterpret_cast<const uint8_t*>(input);

// Create Arrow Tensor Object, no copy made!
// {input_length} is the shape of the tensor
auto value_buffer = std::make_shared<Buffer>(bytes_array, sizeof(float)*input_length);
Tensor t(float32(), value_buffer, {input_length});
```

Step 2: Save the Arrow Tensor to Plasma In-Memory Object Store
--------------------------------------------------------------
Continuing from Step 1, this step will save the tensor to Plasma Store. We
use `arrow::ipc::WriteTensor` to write the data. 

Note that the `meta_len` is set to `0` for a Tensor message. 

```cpp
// Get the size of tensor for Plasma
int64_t datasize;
ARROW_CHECK_OK(ipc::GetTensorSize(t, &datasize));
int32_t meta_len = 0;

// Create Plasma Object
// Plasma is responsible for initializing and resizing the buffer
// This buffer will contain _serialized_ tensor
std::shared_ptr<Buffer> buffer;
ARROW_CHECK_OK(
    client_.Create(object_id, datasize, NULL, 0, &buffer));

// Writing Process, Copy made!
io::FixedSizeBufferWriter stream(buffer);
ARROW_CHECK_OK(arrow::ipc::WriteTensor(t, &stream, &meta_len, &datasize));

// Seal Plasma Object
// Perform a hash by default 
// (Clipper turned it off by making build a custom fork of arrow)
ARROW_CHECK_OK(client_.Seal(object_id));
```

Step 3: Access the Tensor in a Python Process
---------------------------------------------
In Python, we will construct the Plasma client and point it to the same socket.
The `inputs` variable will be a list of oids in their raw byte string form. 

```python
import pyarrow as pa
import pyarrow.plasma as plasma

plasma_client = plasma.connect('/tmp/plasma','',0)

# inputs: a list of length 20 bytes object ids
# For example
inputs = [b'a'*20]

# Construct Object ID and perform a batch get
oids = [plasma.ObjectID(inp) for inp in inputs]
buffers = plasma_client.get_buffers(oids)

# Read the tensor and convert to numpy array for each object
arrs = []
for buffer in buffers:
    reader = pa.BufferReader(buffer)
    t = pa.read_tensor(reader)
    arr = t.to_numpy()
    arrs.append(arr)
# arrs is now a list of numpy array
```