%CHUNKEDARRAY arrow.array.ChunkedArray

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

classdef ChunkedArray < matlab.mixin.CustomDisplay & ...
                        matlab.mixin.Scalar

    properties(Hidden, SetAccess=private, GetAccess=public)
        Proxy
    end

    properties(Dependent, SetAccess=private, GetAccess=public)
        Type
        NumChunks
        Length
    end

    methods
        function obj = ChunkedArray(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.ChunkedArray")}
            end
            import arrow.internal.proxy.validate
            obj.Proxy = proxy;
        end

        function numChunks = get.NumChunks(obj)
            numChunks = obj.Proxy.getNumChunks();
        end

        function length = get.Length(obj)
            length = obj.Proxy.getLength();
        end

        function type = get.Type(obj)
            typeStruct = obj.Proxy.getType();
            traits = arrow.type.traits.traits(arrow.type.ID(typeStruct.TypeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.TypeProxyClassName, ID=typeStruct.ProxyID);
            type = traits.TypeConstructor(proxy);
        end

        function array = chunk(obj, idx)
            idx = arrow.internal.validate.index.numeric(idx, "int32", AllowNonScalar=false);
            chunkStruct = obj.Proxy.getChunk(struct(Index=idx));
            traits = arrow.type.traits.traits(arrow.type.ID(chunkStruct.TypeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.ArrayProxyClassName, ID=chunkStruct.ProxyID);
            array = traits.ArrayConstructor(proxy);
        end
    end

    methods(Static)
        function chunkedArray = fromArrays(arrays)
            arguments(Repeating)
                arrays(1, 1) arrow.array.Array
            end

            narginchk(1, inf);

            proxyIDs = arrow.array.internal.getArrayProxyIDs(arrays);
            args = struct(ArrayProxyIDs=proxyIDs);
            proxyName = "arrow.array.proxy.ChunkedArray";
            proxy = arrow.internal.proxy.create(proxyName, args);

            chunkedArray = arrow.array.ChunkedArray(proxy);
        end
    end 
end