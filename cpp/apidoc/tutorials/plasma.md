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

Using the Plasma In-Memory Object Store
=======================================

Apache Arrow offers the ability to share your data structures among multiple 
processes simultaneously through Plasma, an in-memory object store.

Plasma object stores can be local, as in being on the same node, or remote. 
Plasma can communicate between local and remote object stores to share 
objects between nodes as well.

Like in Apache Arrow, Plasma objects are immutable. 

The following goes over the basics so you can begin using Plasma in your big 
data applications.

Starting up Plasma
------------------

Any process trying to access the Plasma object store needs to be set up as a 
Plasma client. To start running the Plasma object store so that clients may 
connect, you'll need to first locate your plasma directory. 

Most likely, your plasma directory is inside your anaconda installation. If you 
have python installed in your machine, you can easily find out the location of 
your plasma directory by running the following one-liner from the terminal:

```
ubuntu:~$ python -c "import plasma; print(plasma.__path__)"
['/home/ubuntu/anaconda3/lib/python3.6/site-packages/plasma-0.0.1-py3.6-linux-x86_64.egg/plasma']

```

Cd into your plasma directory. To start running the plasma object store, you can 
run the `plasma_store` process in the foreground with a terminal command similar 
to below:

```
./plasma_store -m 1000000000 -s /tmp/plasma

```

This command only works from inside the plasma directory where the `plasma_store` 
executable is located. This command takes in two flags-- the -m flag specifies 
the size of the object store in bytes, and the -s flag specifies the filepath of 
the UNIX domain socket that the store will listen at.

Therefore, the above command initializes a Plasma store up to 1 GB of memory, and 
sets the socket to `/tmp/plasma.`

The Plasma store will remain available as long as the `plasma_store` process is 
running in a terminal window. Messages, such as alerts for disconnecting clients,
may occasionally be outputted. To stop running the Plasma store, you can press
`CTRL-C` in the terminal window.

Alternatively, you can run the Plasma store in the background and ignore all 
message output with the following terminal command:

```
./plasma_store -m 1000000000 -s /tmp/plasma 1> /dev/null 2> /dev/null &

```

The Plasma store will instead run silently in the background without having your 
current window hang. To stop running the Plasma store in this case, issue the 
below terminal command:

```
killall plasma_store &

```

Creating a Plasma client
------------------------

Now that the Plasma object store is up and running, it's time to make client 
processes (such as an instance of your C++ program) connect to it. To use the 
Plasma object store as a client, your application should initialize a 
`plasma::PlasmaClient` object and tell it to connect to socket specified when 
starting up the Plasma object store.

```
#include <plasma/client.h>
#include <plasma/common.h>
#include <plasma/plasma.h>
#include <plasma/protocol.h>
using namespace plasma;

int main(int argc, char** argv) {
    // Start up and connect a Plasma client.
    PlasmaClient client_;
    client_.Connect("/tmp/plasma", "", PLASMA_DEFAULT_RELEASE_DELAY);
}

```

Note that multiple clients can be created within the same process, and 
clients can be created among multiple concurrent processes.


Object IDs
----------

The Plasma object store uses a key-value system for accessing objects stored 
in the shared memory. Each object in the Plasma store should be associated 
with a unique id. The Object ID then serves as a key for *any* client to fetch 
that object from the Plasma store. You can form an ``ObjectID`` object from a 
byte string of 20 bytes.

```
// Create an Object ID.
ObjectID object_id;
uint8_t* data = id.mutable_data();

// Write out the byte string 'aaaaaaaaaaaaaaaaaaaa'.
// plasma::kUniqueIDSize = 20
for (int i = 0; i < kUniqueIDSize; i++) {
    data[i] = (uint8_t)'a';
}

```

Random generation of Object IDs is often good enough to ensure unique ids. 
Alternatively, you can simply create a random Object ID as follows:

```
// Randomly generate an Object ID.
ObjectID object_id = ObjectID::from_random();

```

Now, any connected client that knows the object's Object ID can access the 
same object from the Plasma object store. For easy transportation of Object IDs, 
you can convert/serialize an Object ID into a binary string and back as 
follows:

```
// From ObjectID to binary string
std:string id_string = object_id.binary();

// From binary string to ObjectID
ObjectID id_object = ObjectID::from_binary(&id_string);

```

Creating an Object
------------------

Now that you have an Object ID to refer with, you can now create an object 
to store into Plasma with that Object ID.

Objects are created in Plasma in two stages. First, they are *created*, in 
which you specify a pointer for which the object's data and contents will be 
constructed from. At this point, the client can still modify the contents 
of the data array.

To create an object for Plasma, you need to create an object id, as well as 
give the object's maximum data size in bytes. All metadata for the object 
should be passed in at point of creation as well:

```
// Create the Plasma object by specifying its size and metadata.
int64_t data_size = 100;
uint8_t metadata[] = {5};
int64_t metadata_size = sizeof(metadata);
uint8_t* data;
client_.Create(object_id, data_size, metadata, metadata_size, &data);

```

If there is no metadata for the object, you should pass in NULL instead:

```
// Create a Plasma object without metadata.
int64_t data_size = 100;
uint8_t* metadata = NULL;
int64_t metadata_size = 0;
uint8_t* data;
client_.Create(object_id, data_size, metadata, metadata_size, &data);

```

Now that we've specified the pointer to our object's data, we can 
write our data to it:

```
// Write the data for the Plasma object.
for (int64_t i = 0; i < data_size; i++) {
    data[i] = static_cast<uint8_t>(i % 4);
}

```

When the client is done, the client *seals* the buffer, making the object 
immutable, and making it available to other Plasma clients:

```
// Seal the object. This makes it available for all clients.
client_.Seal(object_id);

```

To verify that an object exists in the Plasma object store, you can 
call `PlasmaClient::Contains()` to check if an object has 
been created and sealed for a given Object ID. Note that this function 
will still return False if the object has been created, but not yet 
sealed:

```
// Check if an object has been created and sealed.
bool has_object;
client_.Contains(object_id, &has_object);

```

Getting an Object
-----------------

After an object has been sealed, any client who knows the Object ID can get 
the object. To store the retrieved object contents, you should create an 
`ObjectBuffer,` then call `PlasmaClient::Get()` as follows:

```
// Get from the Plasma store by Object ID.
ObjectBuffer object_buffer;
client_.Get(&object_id, 1, -1, &object_buffer);

```

`PlasmaClient::Get()` isn't limited to fetching a single object 
from the Plasma store at once. You can specify an array of Object IDs and 
`ObjectBuffers` to fetch at once, so long as you also specify the 
number of objects being fetched:

```
// Get two objects at once from the Plasma store. This function 
// call will block until both objects have been fetched.
ObjectBuffer multiple_buffers[2];
ObjectID multiple_ids[2] = {object_id1, object_id2};
int64_t number_of_objects = 2;
client_.Get(multiple_ids, number_of_objects, -1, multiple_buffers);

```

Since `PlasmaClient::Get()` is a blocking function call, it may be 
necessary to limit the amount of time the function is allowed to take
when trying to fetch from the Plasma store. You can pass in a timeout 
in milliseconds when calling `PlasmaClient::Get().` To use `PlasmaClient::Get()` 
without a timeout, just pass in -1 like in the previous example calls:

```
// Make the function call give up fetching the object if it takes 
// more than 100 milliseconds.
int64_t timeout = 100;
client_.Get(object_id, 1, timeout, object_buffer);

```

Finally, to reconstruct the object, you can access the `data` and 
`metadata` attributes of the `ObjectBuffer.` The `data` can be indexed 
like any array:

```
// Reconstruct object data
uint8_t* retrieved_data = object_buffer.data;
uint8_t retrieved_data_length = object_buffer.data_size;

// Reconstruct object metadata
uint8_t* retrieved_metadata = object_buffer.metadata;
uint8_t retrieved_metadata_length = object_buffer.metadata_size;

// Index into data array
uint8_t first_data_byte = retrieved_data[0];

```

Working with Remote Plasma Stores
---------------------------------

So far, we've worked with making our client store and get from the 
local Plasma store instance. This is enough if we want to share our 
data among processes on the same node/machine. However, if we want 
to share data across networks, we'll have to expand our API a little.

*   ** Transfer Objects to a Remote Plasma Instance**

    If we know the IP address and port of a remote Plasma manager, we can
    transfer a local object over to the remote Plasma store as follows:

    ```
    // Transferring an object to a remote Plasma manager.
    const char* addr = "192.168.0.25";  // Dummy value
    int port = 50108;  // Dummy value
    client_.Transfer(addr, port, &object_id);

    ```

*   ** Fetching Objects from Remote Plasma Stores**
    
    If we know their Object IDs, we can attempt to fetch objects from remote 
    Plasma managers into our local Plasma store by calling `PlasmaClient::Fetch().` 
    This method is safe in that it is non-blocking, checks if the object is in the 
    local object store already, and can be called multiple times without side effects.

    ```
    // Fetching an object from remote Plasma managers.
    int number_of_ids = 5;
    ObjectID obj_ids[5] = {obj_id1, obj_id2, obj_id3, obj_id4, obj_id5};
    client_.Fetch(number_of_ids, obj_ids);

    ```

    Of course, since `PlasmaClient::Fetch()` is non-blocking, the objects won't 
    necessarily be ready right after you call the function. This is where the next 
    section of this tutorial comes in.


Querying Status from Plasma
---------------------------

The power of Plasma is that we are able to share our data structures 
between different processes and even different nodes. However, it may 
be difficult for your process to know what is going with the other processes, 
have objects been stored into Plasma yet, etc. 

Plasma provides the following API to query the status of objects and to 
coordinate among different Plasma clients.

*   **Object Location and Status**

    You can find out the current status of an object in the Plasma store by 
    querying using its Object ID. From the status, you can find out if the 
    object doesn't exist, if the object is in a local vs. a remote Plasma 
    store, and if the object is in the middle of being transferred:

    ```
    // Query the object's status
    int object_status;
    client_.Info(object_id, &object_status);

    switch(object_status) {
        case PLASMA_CLIENT_LOCAL :
            // Object is in a local Plasma store
            break;
        case PLASMA_CLIENT_TRANSFER :
            // Object is being transferred
            break;
        case PLASMA_CLIENT_REMOTE :
            // Object is in a remote Plasma store
            break;
        case PLASMA_CLIENT_DOES_NOT_EXIST :
            // Object does not exist in the system
            break;
    }

    ```
*   **Sealed Object Notifications**

    Additionally, you can arrange Plasma to notify you when objects are 
    sealed in the object store. This may especially be handy when your 
    program is collaborating with other Plasma clients, and needs to know 
    when they make objects available.

    First, you can subscribe your current Plasma client to such notifications 
    by getting a file descriptor:

    ```
    // Start receiving notifications into file_descriptor.
    int file_descriptor;
    client_.Subscribe(&fd);

    ```

    Once you have the file descriptor, you can have your current Plasma client 
    wait to receive the next object notification. Object notifications 
    include information such as Object ID, data size, and metadata size of 
    the next newly available object:

    ```
    // Receive notification of the next newly available object. 
    // Notification information is stored in new_object_id, new_data_size, and new_metadata_size
    ObjectID new_object_id;
    int64_t new_data_size;
    int64_t new_metadata_size;
    client_.GetNotification(file_descriptor, &new_object_id, &new_data_size, &new_metadata_size);

    // Fetch the newly available object.
    ObjectBuffer object_buffer;
    client_.Get(&new_object_id, 1, -1, &object_buffer);

    ```

*   **Waiting for Objects to be Ready**

    If your program already has the Object IDs from other clients that it wants to 
    process (whether they be in a local or remote store), however said objects have 
    yet to be sealed, you can instead call `PlasmaClient::Wait()` to block your program's 
    control flow until the objects have been sealed. 

    For each object desired, you have to form an `ObjectRequest` from its Object ID 
    as follows:

    ```
    // Request the objects by Object ID by forming ObjectRequests
    ObjectRequest obj1; 
    obj1.object_id = obj_ID_1;
    obj1.type = PLASMA_QUERY_ANYWHERE;

    ```

    You can specify an `ObjectRequest` to wait for an object anywhere, or to 
    wait for an object from its local object store. The latter would be 
    created instead as follows:

    ```
    ObjectRequest obj2; 
    obj2.object_id = obj_ID_2;
    obj2.type = PLASMA_QUERY_LOCAL;

    ```

    You can also form an `ObjectRequest` to wait for any object in general, and 
    not for a particular Object ID as follows:

    ```
    ObjectRequest obj3; 
    obj3.object_id = ID_NIL;
    obj3.type = PLASMA_QUERY_ANYWHERE;

    ```

    Once you have formed your `ObjectRequests,` you can call `PlasmaClient.Wait()`:

    ```
    ObjectRequest requests[3] = {obj1, obj2, obj3};

    // Block until 2 of 3 desired objects become available.
    int64_t num_of_desired_objects = 3;
    int64_t num_of_objects_min = 2;

    // Where to return how many objects did successfully become available.
    int64_t num_of_objects_satisfied;

    client_.Wait(num_of_desired_objects, requests, num_of_objects_min, -1, &num_of_objects_satisfied);

    ```

Finish Using Objects in Plasma
------------------------------

*   **Releasing Objects from Get**

    Once your client is done with using an object in the Plasma store, you should 
    call `PlasmaClient::Release()` to notify Plasma. `PlasmaClient::Release()` 
    should be called once for every call made to `PlasmaClient::Get()` for this 
    specific Object ID. Note that after calling this function, the address 
    returned by `PlasmaClient::Get()` will no longer be valid.

    ```
    // Free the fetched object from the client.
    client_.Release(object_id);

    ```

*   **Delete Objects from the Plasma Store**

    You can also choose to delete an object from the Plasma object store entirely. 
    This should only be done for objects that are present and sealed:

    ```
    // Verify object is present and sealed first
    bool has_object;
    client_.Contains(object_id, &has_object);

    if (has_object) {
        // Delete object by Object ID
        client_.Delete(object_id);
    }

    ```

*   **Clearing Memory from the Plasma Store**

    Occasionally, the Plasma store may become too full if not allocated enough 
    memory, and creating new objects in Plasma will fail. You can check if 
    the Plasma store is full by checking the `arrow::Status` that `PlasmaClient::Create` 
    returns. 

    If the Plasma store is too full, you can force Plasma to try to clear up 
    a given amount of memory (in bytes) by asking it to delete objects that 
    haven't been used in a while:

    ```
    // Attempt to create a new object
    int64_t data_size2 = 100;
    uint8_t* metadata2 = NULL;
    int64_t metadata_size2 = 0;
    uint8_t* data2;
    Status returnStatus = client_.Create(object_id2, data_size2, metadata2, metadata_size2, &data2);

    // If Plasma is too full, evict to make more room
    if (returnStatus.IsPlasmaStoreFull()) {
        num_bytes = data_size2 + metadata_size2;
        int64_t bytes_successfully_evicted;
        client_.Evict(num_bytes, &bytes_successfully_evicted);
    }

    ```

Shutting Down Plasma
--------------------

*   **Disconnecting the Client from the Local Plasma Store**

    Once your program finishes using the Plasma object store, you should disconnect 
    your client as follows:

    ```
    // Disconnect the client from the Plasma store's socket.
    client_.Disconnect();

    ```

*    **Shut Down the Plasma Object Store**

    Finally, to shut down the Plasma object store itself, you can terminate the 
    `plasma_store` process from within your C++ program as follows:

    ```
    // Shut down the Plasma object store.
    system("killall plasma_store &");

    ```

