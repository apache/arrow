%CREATETABLEWITHSUPPORTEDTYPES Creates a MATLAB table containing all the
%MATLAB types that can be converted into arrow arrays.

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

function T = createTableWithSupportedTypes(opts)
    arguments
        opts.NumRows(1, 1) {mustBeFinite, mustBeNonnegative} = 3;  
    end

    numRows = opts.NumRows;

    variableNames = ["uint8", ...
                     "uint16", ...
                     "uint32", ...
                     "uint64", ...
                     "int8", ...
                     "int16", ...
                     "int32", ...
                     "int64", ...
                     "logical", ...
                     "single", ...
                     "double", ...
                     "string", ...
                     "datetime"];

    T = table(uint8   ((1:numRows)'), ...
              uint16  ((1:numRows)'), ...
              uint32  ((1:numRows)'), ...
              uint64  ((1:numRows)'), ...
              int8    ((1:numRows)'), ...
              int16   ((1:numRows)'), ...
              int32   ((1:numRows)'), ...
              int64   ((1:numRows)'), ...
              logical ((1:numRows)'), ...
              single  ((1:numRows)'), ...
              double  ((1:numRows)'), ...
              string  ((1:numRows)'), ...
              datetime(2023, 6, 28) + days(0:numRows-1)', ...
              VariableNames=variableNames);
end