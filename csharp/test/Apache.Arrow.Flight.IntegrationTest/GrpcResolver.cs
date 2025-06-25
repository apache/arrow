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

using Grpc.Net.Client.Balancer;

namespace Apache.Arrow.Flight.IntegrationTest;

/// <summary>
/// The Grpc.Net.Client library doesn't know how to handle the "grpc+tcp" scheme used by Arrow Flight.
/// This ResolverFactory passes these through to the standard Static Resolver used for the http scheme.
/// </summary>
public class GrpcTcpResolverFactory : ResolverFactory
{
    public override string Name => "grpc+tcp";

    public override Resolver Create(ResolverOptions options)
    {
        return new StaticResolverFactory(
                uri => new[] { new BalancerAddress(options.Address.Host, options.Address.Port) })
            .Create(options);
    }
}
