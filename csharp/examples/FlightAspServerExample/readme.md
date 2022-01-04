<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Flight Asp Server Example

This example flight service stores uploaded Arrow tables in memory.

## How this was created

First, create a new gRPC service (Flight is based on gRPC):

```
dotnet new grpc
```

Delete the example greeter service.

Then, implement a concrete subclass of [`FlightServer`](../../src/Apache.Arrow.Flight/Server/FlightServer.cs). 
See [./Services/InMemoryFlightServer.cs](./Services/InMemoryFlightServer.cs).

Finally, in [./Program.cs](./Program.cs) add the Flight server to the gRPC services and 
map the endpoints with the extension methods:

```csharp
grpcBuilder.AddFlightServer<InMemoryFlightServer>();
...
app.MapFlightEndpoint();
```
