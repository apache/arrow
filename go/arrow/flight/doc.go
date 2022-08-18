// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Here we list best practices and common pitfalls for Arrow Flight usage.
//
// GRPC
//
// When using gRPC for transport all client methods take an optional list
// of gRPC CallOptions: https://pkg.go.dev/google.golang.org/grpc#CallOption.
// Additional headers can be used or read via
// https://pkg.go.dev/google.golang.org/grpc@v1.48.0/metadata with the context.
// Also see available gRPC keys
// (https://grpc.github.io/grpc/cpp/group__grpc__arg__keys.html) and a list of
// best gRPC practices (https://grpc.io/docs/guides/performance/#general).
//
// Re-use clients whenever possible
//
// Closing clients causes gRPC to close and clean up connections which can take
// several seconds per connection. This will stall server and client threads if
// done too frequently. Client reuse will avoid this issue.
//
// Don’t round-robin load balance
//
// Round robin balancing can cause every client to have an open connection to
// every server causing an unexpected number of open connections and a depletion
// of resources.
//
// Debugging
//
// Use netstat to see the number of open connections.
// For debug use env GODEBUG=http2debug=1 or GODEBUG=http2debug=2 for verbose
// http2 logs (using 2 is more verbose with frame dumps). This will print the
// initial headers (on both sides) so you can see if grpc established the
// connection or not. It will also print when a message is sent, so you can tell
// if the connection is open or not.
// Note: "connect" isn't really a connect and we’ve observed that gRPC does not
// give you the actual error until you first try to make a call. This can cause
// error being reported at unexpected times.
//
// Use ListFlights sparingly
//
// ListFlights endpoint is largely just implemented as a normal GRPC stream
// endpoint and can hit transfer bottlenecks if used too much. To estimate data
// transfer bottleneck:
//
// 5k schemas will serialize to about 1-5 MB/call. Assuming a gRPC localhost
// bottleneck of 3GB/s you can at best serve 600-3000 clients/s.
//
// https://issues.apache.org/jira/browse/ARROW-15764 proposes a caching
// optimisation for server side, but it was not yet implemented.
//
//
// Memory cache client-side
//
// Flight uses gRPC allocator wherever possible.
//
// gRPC will spawn an unbounded number of threads for concurrent clients. Those
// threads are not necessarily cleaned up (cached thread pool in java parlance).
// glibc malloc clears some per thread state and the default tuning never clears
// caches in some workloads. But you can explicitly tell malloc to dump caches.
// See https://issues.apache.org/jira/browse/ARROW-16697 as an example.
//
// A quick way of testing: attach to the process with a debugger and call malloc_trim
//
//
// Excessive traffic
//
// There are basically two ways to handle excessive traffic:
// * unbounded thread pool -> everyone gets serviced, but it might take forever.
// This is what you are seeing now.
// bounded thread pool -> Reject connections / requests when under load, and have
// clients retry with backoff. This also gives an opportunity to retry with a
// different node. Not everyone gets serviced but quality of service stays consistent.
//
// Closing unresponsive connections
//
// * A stale connection can be closed using CallOptions.stop_token. This requires
// recording the stop token at connection establishment time.
// * Use client timeout
// * here is a long standing ticket for a per-write/per-read timeout instead of a per
// call timeout (https://issues.apache.org/jira/browse/ARROW-6062), but this is not
// (easily) possible to implement with the blocking gRPC API. For now one can also do
// something like set up a background thread that calls cancel() on a timer and have
// the main thread reset the timer every time a write operation completes successfully
// (that means one needs to use to_batches() + write_batch and not write_table).
package flight