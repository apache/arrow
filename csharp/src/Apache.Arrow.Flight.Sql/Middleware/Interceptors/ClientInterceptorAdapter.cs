// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Flight.Sql.Middleware.Grpc;
using Apache.Arrow.Flight.Sql.Middleware.Interfaces;
using Apache.Arrow.Flight.Sql.Middleware.Models;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Apache.Arrow.Flight.Sql.Middleware.Interceptors
{
    public class ClientInterceptorAdapter : Interceptor
    {
        private readonly IList<IFlightClientMiddlewareFactory> _factories;

        public ClientInterceptorAdapter(IEnumerable<IFlightClientMiddlewareFactory> factories)
        {
            _factories = factories.ToList();
        }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
            where TRequest : class
            where TResponse : class
        {
            var middleware = new List<IFlightClientMiddleware>();
            var callInfo = new CallInfo(
                context.Host ?? "unknown",
                FlightMethodParser.ParseMethodName(context.Method.FullName));

            try
            {
                middleware.AddRange(_factories.Select(factory => factory.OnCallStarted(callInfo)));
            }
            catch (Exception e)
            {
                throw new RpcException(new Status(StatusCode.Internal, "Middleware creation failed"), e.Message);
            }

            AddCallerMetadata(ref context);

            // Apply middleware headers
            var headers = context.Options.Headers ?? new Metadata();
            var adapter = new MetadataAdapter(headers);
            foreach (var m in middleware)
            {
                m.OnBeforeSendingHeaders(adapter);
            }

            // Merge original headers with middleware headers
            var mergedHeaders = new Metadata();
            if (context.Options.Headers != null)
            {
                foreach (var entry in context.Options.Headers)
                {
                    mergedHeaders.Add(entry);
                }
            }

            var updatedContext = new ClientInterceptorContext<TRequest, TResponse>(
                context.Method,
                context.Host,
                context.Options.WithHeaders(mergedHeaders)
            );

            var headersReceived = false;
            var call = continuation(request, updatedContext);

            var responseHeadersTask = call.ResponseHeadersAsync.ContinueWith(task =>
            {
                if (task.Exception is null)
                {
                    var metadataAdapter = new MetadataAdapter(task.Result);
                    middleware.ForEach(m => m.OnHeadersReceived(metadataAdapter));
                    headersReceived = true;
                }

                return task.Result;
            });

            var responseTask = call.ResponseAsync.ContinueWith(response =>
            {
                // If headers were never received, simulate with trailers
                if (!headersReceived)
                {
                    var trailersAdapter = new MetadataAdapter(call.GetTrailers());
                    foreach (var m in middleware)
                        m.OnHeadersReceived(trailersAdapter);
                }

                var status = call.GetStatus();
                var trailers = call.GetTrailers();
                var flightStatus = StatusUtils.FromGrpcStatusAndTrailers(status, trailers);

                middleware.ForEach(m => m.OnCallCompleted(flightStatus));

                if (response.IsFaulted && response.Exception != null)
                    throw response.Exception;

                return response.Result;
            });

            return new AsyncUnaryCall<TResponse>(
                responseTask,
                responseHeadersTask,
                call.GetStatus,
                call.GetTrailers,
                call.Dispose);
        }

        private void AddCallerMetadata<TRequest, TResponse>(ref ClientInterceptorContext<TRequest, TResponse> context)
            where TRequest : class
            where TResponse : class
        {
            var headers = context.Options.Headers;

            // Call doesn't have a headers collection to add to.  
            // Need to create a new context with headers for the call.
            if (headers == null)
            {
                headers = new Metadata();
                var options = context.Options.WithHeaders(headers);
                context = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, options);
            }

            // Add caller metadata to call headers  
            headers.Add("caller-user", Environment.UserName);
            headers.Add("caller-machine", Environment.MachineName);
            headers.Add("caller-os", Environment.OSVersion.ToString());
        }
    }
}