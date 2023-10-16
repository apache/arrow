%VALIDATEARRAYLENGTHS Validates all arrays in the cell array arrowArrays
%have the same length.

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

function validateArrayLengths(arrowArrays)
    
    numArrays = numel(arrowArrays);

    if numArrays == 0
        return;
    end

    expectedNumElements = arrowArrays{1}.NumElements;

    for ii = 2:numel(arrowArrays)
        if arrowArrays{ii}.NumElements ~= expectedNumElements
            errid = "arrow:tabular:UnequalArrayLengths";
            msg = compose("Expected all arrays to have %d elements," + ...
                " but the array at position %d has %d elements.", ...
                    expectedNumElements, ii, arrowArrays{ii}.NumElements);
            error(errid, msg);
        end
    end
end

