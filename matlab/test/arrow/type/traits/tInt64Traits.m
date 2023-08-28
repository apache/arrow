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

classdef tInt64Traits < hTypeTraits

    properties
        TraitsConstructor = @arrow.type.traits.Int64Traits
        ArrayConstructor = @arrow.array.Int64Array
        ArrayClassName = "arrow.array.Int64Array"
        ArrayProxyClassName = "arrow.array.proxy.Int64Array"
        ArrayStaticConstructor = @arrow.array.Int64Array.fromMATLAB
        TypeConstructor = @arrow.type.Int64Type
        TypeClassName = "arrow.type.Int64Type"
        TypeProxyClassName = "arrow.type.proxy.Int64Type"
        MatlabConstructor = @int64
        MatlabClassName = "int64"
    end

end