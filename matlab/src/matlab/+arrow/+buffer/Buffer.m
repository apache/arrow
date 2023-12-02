%BUFFER Represents a block of contiguous memory with a specified size.

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

classdef Buffer < matlab.mixin.Scalar

    properties (Dependent, GetAccess=public, SetAccess=private)
        NumBytes
    end

    properties (Hidden, GetAccess=public, SetAccess=private)
        Proxy
    end

    methods
        function obj = Buffer(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.buffer.proxy.Buffer")}
            end
            import arrow.internal.proxy.validate
            obj.Proxy = proxy;
        end

        function numBytes = get.NumBytes(obj)
            numBytes = obj.Proxy.getNumBytes();
        end

        function data = toMATLAB(obj)
            data = obj.Proxy.toMATLAB();
        end

        function tf = isequal(obj, varargin)
            narginchk(2, inf);
            tf = false;

            bufferProxyIDs = zeros([1 numel(varargin)], "uint64");
            for ii = 1:numel(varargin)
                maybeBuffer = varargin{ii};
                if ~isa(maybeBuffer, "arrow.buffer.Buffer")
                    % If maybeBuffer is not an instance of
                    % arrow.buffer.Buffer, return false early.
                    return;
                end
                % Otherwise, extract the proxy ID associated with
                % maybeBuffer.
                bufferProxyIDs(ii) = maybeBuffer.Proxy.ID;
            end

            % Invoke the isEqual method on the Buffer Proxy class.
            tf = obj.Proxy.isEqual(bufferProxyIDs);
        end
    end

    methods (Static, Hidden)
        function buffer = fromMATLAB(data)
            arguments
                data(:, 1) {mustBeNumeric, mustBeNonsparse, mustBeReal}
            end

            % Re-interpret bit pattern as uint8s without changing the
            % underlying data.
            data = typecast(data, "uint8");
            args = struct(Values=data);
            proxy = arrow.internal.proxy.create("arrow.buffer.proxy.Buffer", args);
            buffer = arrow.buffer.Buffer(proxy);
        end
    end
end