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

classdef ListTraits < arrow.type.traits.TypeTraits

    properties (Constant)
        ArrayConstructor = @arrow.array.ListArray
        ArrayClassName = "arrow.array.ListArray"
        ArrayProxyClassName = "arrow.array.proxy.ListArray"
        ArrayStaticConstructor = @arrow.array.ListArray.fromMATLAB
        TypeConstructor = @arrow.type.ListType
        TypeClassName = "arrow.type.ListType"
        TypeProxyClassName = "arrow.type.proxy.ListType"
        % The cell function works differently than other
        % "type construction functions" in MATLAB.
        MatlabConstructor = missing
        MatlabClassName = "cell"
    end

end
