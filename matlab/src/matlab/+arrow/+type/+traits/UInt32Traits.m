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

classdef UInt32Traits < arrow.type.traits.TypeTraits

    properties (Constant)
        ArrayConstructor = @arrow.array.UInt32Array
        ArrayClassName = "arrow.array.UInt32Array"
        ArrayProxyClassName = "arrow.array.proxy.UInt32Array"
        ArrayStaticConstructor = @arrow.array.UInt32Array.fromMATLAB
        TypeConstructor = @arrow.type.UInt32Type
        TypeClassName = "arrow.type.UInt32Type"
        TypeProxyClassName = "arrow.type.proxy.UInt32Type"
        MatlabConstructor = @uint32
        MatlabClassName = "uint32"
    end

end