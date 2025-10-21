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

        function array = fromArrays(offsets, values, opts)
            arguments
                offsets (1, 1) arrow.array.Int32Array
                values (1, 1) arrow.array.Array
                opts.Valid
                opts.ValidationMode (1, 1) arrow.array.ValidationMode = arrow.array.ValidationMode.Minimal
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
            % Validate the provided offsets and values.
            proxy.validate(struct(ValidationMode=uint8(opts.ValidationMode)));
            array = arrow.array.ListArray(proxy);
        end

        function array = fromMATLAB(C)
            arguments
                C(:, 1) cell {mustBeNonempty}
            end
            import arrow.array.internal.list.findFirstNonMissingElement
            import arrow.array.internal.list.createValidator

            idx = findFirstNonMissingElement(C);

            if idx == -1
                id = "arrow:array:list:CellArrayAllMissing";
                msg = "The input cell array must contain at least one non-missing" + ...
                    " value to be converted to an Arrow array.";
                error(id, msg);
            end

            validator = createValidator(C{idx});

            numElements = numel(C);
            valid = true([numElements 1]);
            % All elements before the first non-missing value should be 
            % treated as null values.
            valid(1:idx-1) = false;
            offsets = zeros([numElements + 1, 1], "int32");

            for ii = idx:numElements
                element = C{ii};
                if isa(element, "missing")
                    % Treat missing values as null values.
                    valid(ii) = false;
                    offsets(ii + 1) = offsets(ii);
                else
                    validator.validateElement(element);
                    length = validator.getElementLength(element);
                    offsets(ii + 1) = offsets(ii) + length;
                end
            end

            offsetArray = arrow.array(offsets);

            validValueCellArray = validator.reshapeCellElements(C(valid));
            values = vertcat(validValueCellArray{:});
            valueArray = arrow.array(values);

            args = struct(...
                OffsetsProxyID=offsetArray.Proxy.ID, ...
                ValuesProxyID=valueArray.Proxy.ID, ...
                Valid=valid ...
            );
            
            proxyName = "arrow.array.proxy.ListArray";
            proxy = arrow.internal.proxy.create(proxyName, args);
            array = arrow.array.ListArray(proxy);
        end
    end

end
