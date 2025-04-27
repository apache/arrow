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

using Apache.Arrow.Flight.Sql.Middleware.Models;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql.Middleware.Grpc;

public static class StatusUtils
{
    public static CallStatus FromGrpcStatusAndTrailers(Status status, Metadata trailers)
    {
        var code = FromGrpcStatusCode(status.StatusCode);
        return new CallStatus(
            code,
            status.StatusCode != StatusCode.OK ? new RpcException(status, trailers) : null,
            status.Detail,
            trailers
        );
    }
    
    public static FlightStatusCode FromGrpcStatusCode(StatusCode grpcCode)
    {
        return grpcCode switch
        {
            StatusCode.OK => FlightStatusCode.Ok,
            StatusCode.Cancelled => FlightStatusCode.Cancelled,
            StatusCode.Unknown => FlightStatusCode.Unknown,
            StatusCode.InvalidArgument => FlightStatusCode.InvalidArgument,
            StatusCode.DeadlineExceeded => FlightStatusCode.DeadlineExceeded,
            StatusCode.NotFound => FlightStatusCode.NotFound,
            StatusCode.AlreadyExists => FlightStatusCode.AlreadyExists,
            StatusCode.PermissionDenied => FlightStatusCode.PermissionDenied,
            StatusCode.Unauthenticated => FlightStatusCode.Unauthenticated,
            StatusCode.ResourceExhausted => FlightStatusCode.ResourceExhausted,
            StatusCode.FailedPrecondition => FlightStatusCode.FailedPrecondition,
            StatusCode.Aborted => FlightStatusCode.Aborted,
            StatusCode.OutOfRange => FlightStatusCode.OutOfRange,
            StatusCode.Unimplemented => FlightStatusCode.Unimplemented,
            StatusCode.Internal => FlightStatusCode.Internal,
            StatusCode.Unavailable => FlightStatusCode.Unavailable,
            StatusCode.DataLoss => FlightStatusCode.DataLoss,
            _ => FlightStatusCode.Unknown
        };
    }
}