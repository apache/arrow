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

Using the Plasma In-Memory Object Store from C++
================================================

Apache Arrow offers the ability to share your data structures among multiple
processes simultaneously through Plasma, an in-memory object store.

Note that **the Plasma API is not stable**.

Plasma clients are processes that run on the same machine as the object store.
They communicate with the object store over Unix domain sockets, and they read
and write data in the object store through shared memory.

Plasma objects are immutable once they have been created.

The following goes over the basics so you can begin using Plasma in your big
data applications.

Starting the Plasma store
-------------------------

To start running the Plasma object store so that clients may
connect and access the data, run the following command:

```
plasma_store_server -m 1000000000 -s /tmp/plasma
```

The `-m` flag specifies the size of the object store in bytes. The `-s` flag
specifies the path of the Unix domain socket that the store will listen at.

Therefore, the above command initializes a Plasma store up to 1 GB of memory
and sets the socket to `/tmp/plasma.`

The Plasma store will remain available as long as the `plasma_store_server` process is
running in a terminal window. Messages, such as alerts for disconnecting
clients, may occasionally be output. To stop running the Plasma store, you
can press `Ctrl-C` in the terminal window.

Alternatively, you can run the Plasma store in the background and ignore all
message output with the following terminal command:

```
plasma_store_server -m 1000000000 -s /tmp/plasma 1> /dev/null 2> /dev/null &
```

The Plasma store will instead run silently in the background. To stop running
the Plasma store in this case, issue the command below:

```
killall plasma_store_server
```

Creating a Plasma client
------------------------

Now that the Plasma object store is up and running, it is time to make a client
process connect to it. To use the Plasma object store as a client, your
application should initialize a `plasma::PlasmaClient` object and tell it to
connect to the socket specified when starting up the Plasma object store.

```cpp
#include <plasma/client.h>

using namespace plasma;

int main(int argc, char** argv) {
  // Start up and connect a Plasma client.
  PlasmaClient client;
  ARROW_CHECK_OK(client.Connect("/tmp/plasma"));
  // Disconnect the Plasma client.
  ARROW_CHECK_OK(client.Disconnect());
}
```

Save this program in a file `test.cc` and compile it with

```
g++ test.cc `pkg-config --cflags --libs plasma` --std=c++11
```

Note that multiple clients can be created within the same process.

Note that a `PlasmaClient` object is **not thread safe**.

If the Plasma store is still running, you can now execute the `a.out` executable
and the store will print something like

```
Disconnecting client on fd 5
```

which shows that the client was successfully disconnected.

Object IDs
----------

The Plasma object store uses twenty-byte identifiers for accessing objects
stored in shared memory. Each object in the Plasma store should be associated
with a unique ID. The Object ID is then a key that can be used by **any** client
to fetch that object from the Plasma store.

Random generation of Object IDs is often good enough to ensure unique IDs.
For test purposes, you can use the function `random_object_id` from the header
`plasma/test-util.h` to generate random Object IDs, which uses a global random
number generator. In your own applications, we recommend to generate a string of
`ObjectID::size()` many random bytes using your own random number generator
and pass them to `ObjectID::from_bytes` to generate the ObjectID.

```cpp
#include <plasma/test-util.h>

// Randomly generate an Object ID.
ObjectID object_id = random_object_id();
```

Now, any connected client that knows the object's Object ID can access the
same object from the Plasma object store. For easy transportation of Object IDs,
you can convert/serialize an Object ID into a binary string and back as
follows:

```cpp
// From ObjectID to binary string
std:string id_string = object_id.binary();

// From binary string to ObjectID
ObjectID id_object = ObjectID::from_binary(&id_string);
```

You can also get a human readable representation of ObjectIDs in the same
format that git uses for commit hashes by running `ObjectID::hex`.

Here is a test program you can run:

```cpp
#include <iostream>
#include <string>
#include <plasma/client.h>
#include <plasma/test-util.h>

using namespace plasma;

int main(int argc, char** argv) {
  ObjectID object_id1 = random_object_id();
  std::cout << "object_id1 is " << object_id1.hex() << std::endl;

  std::string id_string = object_id1.binary();
  ObjectID object_id2 = ObjectID::from_binary(id_string);
  std::cout << "object_id2 is " << object_id2.hex() << std::endl;
}
```

Creating an Object
------------------

Now that you learned about Object IDs that are used to refer to objects,
let's look at how objects can be stored in Plasma.

Storing objects is a two-stage process. First a buffer is allocated with a call
to `Create`. Then it can be constructed in place by the client. Then it is made
immutable and shared with other clients via a call to `Seal`.

The `Create` call blocks while the Plasma store allocates a buffer of the
appropriate size. The client will then map the buffer into its own address
space. At this point the object can be constructed in place using a pointer that
was written by the `Create` command.

```cpp
int64_t data_size = 100;
// The address of the buffer allocated by the Plasma store will be written at
// this address.
uint8_t* data;
// Create a Plasma object by specifying its ID and size.
ARROW_CHECK_OK(client.Create(object_id, data_size, NULL, 0, &data));
```

You can also specify metadata for the object; the third argument is the
metadata (as raw bytes) and the fourth argument is the size of the metadata.

```cpp
// Create a Plasma object with metadata.
int64_t data_size = 100;
std::string metadata = "{'author': 'john'}";
uint8_t* data;
client.Create(object_id, data_size, (uint8_t*) metadata.data(), metadata.size(), &data);
```

Now that we've obtained a pointer to our object's data, we can
write our data to it:

```cpp
// Write some data for the Plasma object.
for (int64_t i = 0; i < data_size; i++) {
    data[i] = static_cast<uint8_t>(i % 4);
}
```

When the client is done, the client **seals** the buffer, making the object
immutable, and making it available to other Plasma clients:

```cpp
// Seal the object. This makes it available for all clients.
client.Seal(object_id);
```

Here is an example that combines all these features:

```cpp
#include <plasma/client.h>

using namespace plasma;

int main(int argc, char** argv) {
  // Start up and connect a Plasma client.
  PlasmaClient client;
  ARROW_CHECK_OK(client.Connect("/tmp/plasma"));
  // Create an object with a fixed ObjectID.
  ObjectID object_id = ObjectID::from_binary("00000000000000000000");
  int64_t data_size = 1000;
  std::shared_ptr<Buffer> data;
  std::string metadata = "{'author': 'john'}";
  ARROW_CHECK_OK(client.Create(object_id, data_size, (uint8_t*) metadata.data(), metadata.size(), &data));
  // Write some data into the object.
  auto d = data->mutable_data();
  for (int64_t i = 0; i < data_size; i++) {
    d[i] = static_cast<uint8_t>(i % 4);
  }
  // Seal the object.
  ARROW_CHECK_OK(client.Seal(object_id));
  // Disconnect the client.
  ARROW_CHECK_OK(client.Disconnect());
}
```

This example can be compiled with

```
g++ create.cc `pkg-config --cflags --libs plasma` --std=c++11 -o create
```

To verify that an object exists in the Plasma object store, you can
call `PlasmaClient::Contains()` to check if an object has
been created and sealed for a given Object ID. Note that this function
will still return False if the object has been created, but not yet
sealed:

```cpp
// Check if an object has been created and sealed.
bool has_object;
client.Contains(object_id, &has_object);
if (has_object) {
    // Object has been created and sealed, proceed
}
```

Getting an Object
-----------------

After an object has been sealed, any client who knows the Object ID can get
the object. To store the retrieved object contents, you should create an
`ObjectBuffer`, then call `PlasmaClient::Get()` as follows:

```cpp
// Get from the Plasma store by Object ID.
ObjectBuffer object_buffer;
client.Get(&object_id, 1, -1, &object_buffer);
```

`PlasmaClient::Get()` isn't limited to fetching a single object
from the Plasma store at once. You can specify an array of Object IDs and
`ObjectBuffers` to fetch at once, so long as you also specify the
number of objects being fetched:

```cpp
// Get two objects at once from the Plasma store. This function
// call will block until both objects have been fetched.
ObjectBuffer multiple_buffers[2];
ObjectID multiple_ids[2] = {object_id1, object_id2};
client.Get(multiple_ids, 2, -1, multiple_buffers);
```

Since `PlasmaClient::Get()` is a blocking function call, it may be
necessary to limit the amount of time the function is allowed to take
when trying to fetch from the Plasma store. You can pass in a timeout
in milliseconds when calling `PlasmaClient::Get().` To use `PlasmaClient::Get()`
without a timeout, just pass in -1 like in the previous example calls:

```cpp
// Make the function call give up fetching the object if it takes
// more than 100 milliseconds.
int64_t timeout = 100;
client.Get(&object_id, 1, timeout, &object_buffer);
```

Finally, to access the object, you can access the `data` and
`metadata` attributes of the `ObjectBuffer`. The `data` can be indexed
like any array:

```cpp
// Access object data.
uint8_t* data = object_buffer.data;
int64_t data_size = object_buffer.data_size;

// Access object metadata.
uint8_t* metadata = object_buffer.metadata;
uint8_t metadata_size = object_buffer.metadata_size;

// Index into data array.
uint8_t first_data_byte = data[0];
```

Here is a longer example that shows these capabilities:

```cpp
#include <plasma/client.h>

using namespace plasma;

int main(int argc, char** argv) {
  // Start up and connect a Plasma client.
  PlasmaClient client;
  ARROW_CHECK_OK(client.Connect("/tmp/plasma"));
  ObjectID object_id = ObjectID::from_binary("00000000000000000000");
  ObjectBuffer object_buffer;
  ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));

  // Retrieve object data.
  auto buffer = object_buffer.data;
  const uint8_t* data = buffer->data();
  int64_t data_size = buffer->size();

  // Check that the data agrees with what was written in the other process.
  for (int64_t i = 0; i < data_size; i++) {
    ARROW_CHECK(data[i] == static_cast<uint8_t>(i % 4));
  }

  // Disconnect the client.
  ARROW_CHECK_OK(client.Disconnect());
}
```

If you compile it with

```
g++ get.cc `pkg-config --cflags --libs plasma` --std=c++11 -o get
```

and run it with `./get`, all the assertions will pass if you run the `create`
example from above on the same Plasma store.


Object Lifetime Management
--------------------------

The Plasma store internally does reference counting to make sure objects that
are mapped into the address space of one of the clients with `PlasmaClient::Get`
are accessible. To unmap objects from a client, call `PlasmaClient::Release`.
All objects that are mapped into a clients address space will automatically
be released when the client is disconnected from the store (this happens even
if the client process crashes or otherwise fails to call `Disconnect`).

If a new object is created and there is not enough space in the Plasma store,
the store will evict the least recently used object (an object is in use if at
least one client has gotten it but not released it).

Object notifications
--------------------

Additionally, you can arrange to have Plasma notify you when objects are
sealed in the object store. This may especially be handy when your
program is collaborating with other Plasma clients, and needs to know
when they make objects available.

First, you can subscribe your current Plasma client to such notifications
by getting a file descriptor:

```cpp
// Start receiving notifications into file_descriptor.
int fd;
ARROW_CHECK_OK(client.Subscribe(&fd));
```

Once you have the file descriptor, you can have your current Plasma client
wait to receive the next object notification. Object notifications
include information such as Object ID, data size, and metadata size of
the next newly available object:

```cpp
// Receive notification of the next newly available object.
// Notification information is stored in object_id, data_size, and metadata_size
ObjectID object_id;
int64_t data_size;
int64_t metadata_size;
ARROW_CHECK_OK(client.GetNotification(fd, &object_id, &data_size, &metadata_size));

// Get the newly available object.
ObjectBuffer object_buffer;
ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));
```

Here is a full program that shows this capability:

```cpp
#include <plasma/client.h>

using namespace plasma;

int main(int argc, char** argv) {
  // Start up and connect a Plasma client.
  PlasmaClient client;
  ARROW_CHECK_OK(client.Connect("/tmp/plasma"));

  int fd;
  ARROW_CHECK_OK(client.Subscribe(&fd));

  ObjectID object_id;
  int64_t data_size;
  int64_t metadata_size;
  while (true) {
    ARROW_CHECK_OK(client.GetNotification(fd, &object_id, &data_size, &metadata_size));

    std::cout << "Received object notification for object_id = "
              << object_id.hex() << ", with data_size = " << data_size
              << ", and metadata_size = " << metadata_size << std::endl;
  }

  // Disconnect the client.
  ARROW_CHECK_OK(client.Disconnect());
}
```

If you compile it with

```
g++ subscribe.cc `pkg-config --cflags --libs plasma` --std=c++11 -o subscribe
```

and invoke `./create` and `./subscribe` while the Plasma store is running,
you can observe the new object arriving.
