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
        NumElements
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

        function numElements = get.NumElements(obj)
            numElements = obj.Proxy.getNumElements();
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

        function data = toMATLAB(obj)
            data = preallocateMATLABArray(obj.Type,  obj.NumElements);
            startIndex = 1;
            for ii = 1:obj.NumChunks
                chunk = obj.chunk(ii);
                endIndex = startIndex + chunk.NumElements - 1;
                % Use 2D indexing to support tabular MATLAB types.
                data(startIndex:endIndex, :) = toMATLAB(chunk);
                startIndex = endIndex + 1;
            end
        end

        function tf = isequal(obj, varargin)
            narginchk(2, inf);

            tf = false;
            proxyIDs = zeros(numel(varargin), 1, "uint64");

            % Extract each chunked array's proxy ID
            for ii = 1:numel(varargin)
                chunkedArray = varargin{ii};
                if ~isa(chunkedArray, "arrow.array.ChunkedArray")
                    % Return early if array is not a arrow.array.ChunkedArray
                    return;
                end
                proxyIDs(ii) = chunkedArray.Proxy.ID;
            end

            % Invoke isEqual proxy object method
            tf = obj.Proxy.isEqual(proxyIDs);
        end
    end

    methods(Static)
        function chunkedArray = fromArrays(arrays, opts)
            arguments(Repeating)
                arrays(1, 1) arrow.array.Array
            end
            arguments
                opts.Type(1, 1) arrow.type.Type
            end

            typedProvided = isfield(opts, "Type");

            if (isempty(arrays) && ~typedProvided)
                errid = "arrow:chunkedarray:TypeRequiredWithZeroArrayInputs";
                msg = "Must provide the Type name-value pair if no arrays were provided as input";
                error(errid, msg);
            end

            if typedProvided
                type = opts.Type;
            else
                type = arrays{1}.Type;
            end

            proxyIDs = arrow.array.internal.getArrayProxyIDs(arrays);
            args = struct(ArrayProxyIDs=proxyIDs, TypeProxyID=type.Proxy.ID);
            proxyName = "arrow.array.proxy.ChunkedArray";
            proxy = arrow.internal.proxy.create(proxyName, args);

            chunkedArray = arrow.array.ChunkedArray(proxy);
        end
    end 
end