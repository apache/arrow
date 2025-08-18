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
using System.Threading.Tasks;
using Apache.Arrow.Flight.Middleware.Interfaces;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Apache.Arrow.Flight.Middleware.Interceptors;

public sealed class ClientInterceptorAdapter : Interceptor
{
    private readonly IReadOnlyList<IFlightClientMiddlewareFactory> _factories;

    public ClientInterceptorAdapter(IEnumerable<IFlightClientMiddlewareFactory> factories)
    {
        _factories = factories?.ToList() ?? throw new ArgumentNullException(nameof(factories));
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        var options = InterceptCall(context, out var middlewares);

        var newContext = new ClientInterceptorContext<TRequest, TResponse>(
            context.Method,
            context.Host,
            options);

        var call = continuation(request, newContext);

        return new AsyncUnaryCall<TResponse>(
            HandleResponse(call.ResponseAsync, call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers,
                call.Dispose, middlewares),
            call.ResponseHeadersAsync,
            call.GetStatus,
            call.GetTrailers,
            call.Dispose
        );
    }

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        var callOptions = InterceptCall(context, out var middlewares);
        var newContext = new ClientInterceptorContext<TRequest, TResponse>(
            context.Method, context.Host, callOptions);

        var call = continuation(request, newContext);

        var responseHeadersTask = call.ResponseHeadersAsync.ContinueWith(task =>
        {
            if (task.IsFaulted)
            {
                throw task.Exception!;
            }
            
            if (task.IsCanceled)
            {
                throw new TaskCanceledException(task);
            }
            
            var headers = task.Result;
            var ch = new CallHeaders(headers);
            foreach (var m in middlewares)
                m?.OnHeadersReceived(ch);

            return headers;
        });

        var wrappedResponseStream = new MiddlewareResponseStream<TResponse>(
            call.ResponseStream,
            call,
            middlewares);

        return new AsyncServerStreamingCall<TResponse>(
            wrappedResponseStream,
            responseHeadersTask,
            call.GetStatus,
            call.GetTrailers,
            call.Dispose);
    }


    private CallOptions InterceptCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        out List<IFlightClientMiddleware> middlewareList)
        where TRequest : class
        where TResponse : class
    {
        var callInfo = new CallInfo(context.Method.FullName, context.Method.Type);

        var headers = context.Options.Headers ?? new Metadata();
        middlewareList = new List<IFlightClientMiddleware>();

        var callHeaders = new CallHeaders(headers);

        foreach (var factory in _factories)
        {
            var middleware = factory.OnCallStarted(callInfo);
            middleware?.OnBeforeSendingHeaders(callHeaders);
            middlewareList.Add(middleware);
        }

        return context.Options.WithHeaders(headers);
    }

    private async Task<TResponse> HandleResponse<TResponse>(
        Task<TResponse> responseTask,
        Task<Metadata> headersTask,
        Func<Status> getStatus,
        Func<Metadata> getTrailers,
        Action dispose,
        List<IFlightClientMiddleware> middlewares)
    {
        var nonNullMiddlewares = (middlewares ?? new List<IFlightClientMiddleware>())
            .Where(m => m != null)
            .ToList();

        var hasMiddlewares = nonNullMiddlewares.Count > 0;
        var completionNotified = false;
        
        try
        {
            // Always await headers to surface faults; only materialize CallHeaders if needed.
            var headers = await headersTask.ConfigureAwait(false);
            if (hasMiddlewares)
            {
                var ch = new CallHeaders(headers);
                foreach (var m in nonNullMiddlewares)
                    m.OnHeadersReceived(ch);
            }

            var response = await responseTask.ConfigureAwait(false);

            // Single completion notification
            NotifyCompletionOnce();
            return response;
        }
        catch
        {
            // Completion on failure (only once)
            NotifyCompletionOnce();
            throw;
        }
        finally
        {
            dispose?.Invoke();
        }
        
        void NotifyCompletionOnce()
        {
            if (completionNotified || !hasMiddlewares) return;
            completionNotified = true;

            var status = getStatus();
            var trailers = getTrailers();

            foreach (var m in nonNullMiddlewares)
                m.OnCallCompleted(status, trailers);
        }
    }
}