# Flight Asp Server Example

This is an example flight server that stores uploaded Arrow tables in memory and allows
them to be queried.

## How this was created

Create with steps:

```
dotnet new grpc
```

Implemented flight server at [./FlightAspServerExample/Services/InMemoryFlightServer.cs](./FlightAspServerExample/Services/InMemoryFlightServer.cs).

Add flight endpoints in `Program.cs`.

