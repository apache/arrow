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

classdef ListArray < arrow.array.Array

    properties (Hidden, GetAccess=public, SetAccess=private)
        NullSubstitutionValue = missing;
    end

    properties (Dependent, GetAccess=public, SetAccess=private)
        Values % STUB
        Offsets %STUB
    end

    methods
        
        function obj = ListArray(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.ListArray")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.array.Array(proxy);
        end

        function matlabArray = toMATLAB(obj)
            numElements = obj.NumElements;
            matlabArray = cell(numElements, 1);

            values = toMATLAB(obj.Values);
            offsets = toMATLAB(obj.Offsets) + 1;

            startIndex = offsets(1); 
            for ii = 1:numElements
                endIndex = offsets(ii + 1) - 1;
                matlabArray{ii} = values(startIndex:endIndex, :);
            end

            matlabArray{~obj.Valid} = obj.NullSubstitutionValue;
        end

    end

    methods (Static)
        
        function array = fromMATLAB(~)
            % TODO
        end

    end

end
