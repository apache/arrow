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
        Values
        Offsets
    end

    methods
        
        function obj = ListArray(proxy)
            arguments
                proxy(1, 1) libmexclass.proxy.Proxy {validate(proxy, "arrow.array.proxy.ListArray")}
            end
            import arrow.internal.proxy.validate
            obj@arrow.array.Array(proxy);
        end

        function values = get.Values(obj)
            valueStruct = obj.Proxy.getValues();
            traits = arrow.type.traits.traits(arrow.type.ID(valueStruct.TypeID));
            proxy = libmexclass.proxy.Proxy(Name=traits.ArrayProxyClassName, ID=valueStruct.ProxyID);
            values = traits.ArrayConstructor(proxy);
        end

        function offsets = get.Offsets(obj)
            proxyID = obj.Proxy.getOffsets();
            proxy = libmexclass.proxy.Proxy(Name="arrow.array.proxy.Int32Array", ID=proxyID);
            offsets = arrow.array.Int32Array(proxy);
        end

        function matlabArray = toMATLAB(obj)
            numElements = obj.NumElements;
            matlabArray = cell(numElements, 1);

            values = toMATLAB(obj.Values);
            % Add one to Offsets array because MATLAB
            % uses 1-based indexing.
            offsets = toMATLAB(obj.Offsets) + 1;

            startIndex = offsets(1);
            for ii = 1:numElements
                % Subtract 1 because ending offset value is exclusive.
                endIndex = offsets(ii + 1) - 1;
                matlabArray{ii} = values(startIndex:endIndex, :);
                startIndex = endIndex + 1;
            end

            hasInvalid = ~all(obj.Valid);
            if hasInvalid
                matlabArray(~obj.Valid) = {obj.NullSubstitutionValue};
            end
        end

    end

    methods (Static)
        
        function array = fromMATLAB(C)
            arguments
                C(:, 1) cell
            end

            % Determine the ValueType of the output ListArray by finding
            % the first non-missing value in the input cell array.
            numElements = numel(C);
            indexOfFirstNonMissingValue = -1;
            for ii = 1:numElements
                if ~isa(C{ii}, "missing")
                    indexOfFirstNonMissingValue = ii;
                    break;
                end
            end

            % If the cell array only contains missing (NULL) values, then
            % treat it as a NullArray, rather than a ListArray.
            % TODO: Add NullArray support.
            isNullArray = indexOfFirstNonMissingValue ~= -1;
            assert(isNullArray);

            % Preallocate the Offsets for the output ListArray.
            offsetsDimensions = [numElements + 1, 1];
            offsetsType = "int32";
            offsets = zeros(offsetsDimensions, offsetsType);
            % Preallocate the validity bitmap.
            valid = true(numElements, 1);
            valid(1:indexOfFirstNonMissingValue-1) = false;

            % TODO: Verify the MATLAB type of the first non-missing value
            % is supported for conversion to an Arrow type.
            valueType = string(class(C{indexOfFirstNonMissingValue}));

            % Iterate through the rest of the cell array after the first
            % non-missing value to compute the remaining list Offsets.
            for ii = indexOfFirstNonMissingValue:numElements
                % missing values are treated as NULL elements.
                if isa(C{ii}, "missing")
                    valid(ii) = false;
                    % NULL values are encoded as two consecutive, repeating
                    % offset values.
                    offsets(ii + 1) = offsets(ii);
                elseif isa(C{ii}, valueType)
                    listLength = numel(C{ii});
                    offsets(ii+1) = offsets(ii) + listLength;
                else
                    % TODO: Improve error message for invalid cell arrays.
                    error("arrow:array:list:InvalidCell", "Invalid cell array");
                end
            end

            % Create the Offsets array for the output ListArray.
            offsetsArray = arrow.array.Int32Array.fromMATLAB(offsets);

            % Create the Values array for the output ListArray by vertically
            % concatenating the valid elements and calling the arrow.array
            % gateway function to perform conversion to an Arrow array.

            % To vertically conatenate the valid elements in the cell
            % array, they must all be uniform in shape.
            reshapedCells = cellfun(@(c) reshape(c, [], 1), C(valid), UniformOutput=false);
            values = vertcat(reshapedCells{:});
            valuesArray = arrow.array(values);

            args = struct(...
                OffsetsProxyID=offsetsArray.Proxy.ID, ...
                ValuesProxyID=valuesArray.Proxy.ID, ...
                Valid=valid ...
            );

            proxyName = "arrow.array.proxy.ListArray";
            proxy = arrow.internal.proxy.create(proxyName, args);
            array = arrow.array.ListArray(proxy);
        end

        function array = fromArrays(offsets, values, opts)
            arguments
                offsets (1, 1) arrow.array.Int32Array
                values (1, 1) arrow.array.Array
                opts.Valid
            end
            
            import arrow.internal.validate.parseValid

            if nargin < 2
                error("arrow:array:list:FromArraysValuesAndOffsets", ...
                    "Must supply both an offsets and values array to construct a ListArray.")
            end

            % Offsets should contain one more element than the number of elements in the output ListArray.
            numElements = offsets.NumElements - 1;

            validElements = parseValid(opts, numElements);
            offsetsProxyID = offsets.Proxy.ID;
            valuesProxyID = values.Proxy.ID;

            args = struct(...
                OffsetsProxyID=offsetsProxyID, ...
                ValuesProxyID=valuesProxyID, ...
                Valid=validElements ...
            );
            
            proxyName = "arrow.array.proxy.ListArray";
            proxy = arrow.internal.proxy.create(proxyName, args);
            array = arrow.array.ListArray(proxy);
        end

    end

end
