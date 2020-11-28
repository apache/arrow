// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Threading.Tasks;
using Grpc.Core;

namespace Apache.Arrow.Flight.Client
{
    public class FlightRecordBatchStreamingCall : IDisposable
    {
        private readonly Func<Status> _getStatusFunc;
        private readonly Func<Metadata> _getTrailersFunc;
        private readonly Action _disposeAction;

        internal FlightRecordBatchStreamingCall(
            FlightClientRecordBatchStreamReader recordBatchStreamReader,
            Task<Metadata> responseHeadersAsync,
            Func<Status> getStatusFunc,
            Func<Metadata> getTrailersFunc,
            Action disposeAction)
        {
            ResponseStream = recordBatchStreamReader;
            ResponseHeadersAsync = responseHeadersAsync;
            _getStatusFunc = getStatusFunc;
            _getTrailersFunc = getTrailersFunc;
            _disposeAction = disposeAction;
        }

        public FlightClientRecordBatchStreamReader ResponseStream { get; }

        /// <summary>
        /// Asynchronous access to response headers.
        /// </summary>
        public Task<Metadata> ResponseHeadersAsync { get; }

        /// <summary>
        /// Gets the call status if the call has already finished. Throws InvalidOperationException otherwise.
        /// </summary>
        /// <returns></returns>
        public Status GetStatus()
        {
            return _getStatusFunc();
        }

        /// <summary>
        /// Gets the call trailing metadata if the call has already finished. Throws InvalidOperationException otherwise.
        /// </summary>
        /// <returns></returns>
        public Metadata GetTrailers()
        {
            return _getTrailersFunc();
        }

        /// <summary>
        /// Provides means to cleanup after the call. If the call has already finished normally
        /// (response stream has been fully read), doesn't do anything. Otherwise, requests
        /// cancellation of the call which should terminate all pending async operations
        /// associated with the call. As a result, all resources being used by the call should
        /// be released eventually.
        /// </summary>
        /// <remarks>
        /// Normally, there is no need for you to dispose the call unless you want to utilize
        /// the "Cancel" semantics of invoking Dispose.
        /// </remarks>
        public void Dispose()
        {
            _disposeAction();
        }
    }
}
