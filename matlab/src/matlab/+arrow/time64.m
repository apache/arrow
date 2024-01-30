%TIME64 Creates an arrow.type.Time64Type object

% Licensed to the Apache Software Foundation (ASF) under one or more
% contributor license agreements.  See the NOTICE file distributed with
% this work for additional information regarding copyright ownership.
% The ASF licenses this file to you under the Apache License, Version
% 2.0 (the "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
% implied.  See the License for the specific language governing
% permissions and limitations under the License.

function type = time64(opts)
    arguments
        opts.TimeUnit(1, 1) arrow.type.TimeUnit {timeUnit("Time64", opts.TimeUnit)} = arrow.type.TimeUnit.Microsecond
    end
    import arrow.internal.validate.temporal.timeUnit
    args = struct(TimeUnit=string(opts.TimeUnit));
    proxy = arrow.internal.proxy.create("arrow.type.proxy.Time64Type", args);
    type = arrow.type.Time64Type(proxy);
end
