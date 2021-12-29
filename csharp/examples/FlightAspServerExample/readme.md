# Flight Asp Server Example

This example flight service stores uploaded Arrow tables in memory.

## How this was created

First, create a new gRPC service (Flight is based on gRPC):

```
dotnet new grpc
```

Then, implement a concrete subclass of [`FlightServer`](../../src/Apache.Arrow.Flight/Server/FlightServer.cs). 
See [./Services/InMemoryFlightServer.cs](./Services/InMemoryFlightServer.cs).

Finally, add the Flight server to the gRPC services and map the endpoints with the
extension methods:

```csharp
grpcBuilder.AddFlightServer<InMemoryFlightServer>();
...
app.MapFlightEndpoint();
```

See [./Program.cs](./Program.cs)

