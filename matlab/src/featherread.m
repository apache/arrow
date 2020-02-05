function t = featherread(filename)
%FEATHERREAD Create a table by reading from a Feather file.
%   Use the FEATHERREAD function to create a table by reading
%   column-oriented data from a Feather file.
%
%   T = FEATHERREAD(FILENAME) creates a table by reading from the Feather
%   file FILENAME.

% Licensed to the Apache Software Foundation (ASF) under one
% or more contributor license agreements.  See the NOTICE file
% distributed with this work for additional information
% regarding copyright ownership.  The ASF licenses this file
% to you under the Apache License, Version 2.0 (the
% "License"); you may not use this file except in compliance
% with the License.  You may obtain a copy of the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing,
% software distributed under the License is distributed on an
% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
% KIND, either express or implied.  See the License for the
% specific language governing permissions and limitations
% under the License.

import mlarrow.util.*;

% Validate input arguments.
narginchk(1, 1);
filename = convertStringsToChars(filename);
if ~ischar(filename)
    error('MATLAB:arrow:InvalidFilenameDatatype', ...
        'Filename must be a character vector or string scalar.');
end

% FOPEN can be used to search for files without an extension on the MATLAB
% path.
fid = fopen(filename);
if fid ~= -1
    filename = fopen(fid);
    fclose(fid);
else
    error('MATLAB:arrow:UnableToOpenFile', ...
        'Unable to open file %s.', filename);
end

% Read table variables and metadata from the given Feather file using
% libarrow.
[variables, metadata] = featherreadmex(filename);

% Make valid MATLAB table variable names out of any of the
% Feather table column names that are not valid MATLAB table
% variable names.
[variableNames, variableDescriptions] = makeValidMATLABTableVariableNames({variables.Name});

% Iterate over each table variable, handling invalid (null) entries
% and invalid MATLAB table variable names appropriately.
% Note: All Arrow arrays can have an associated validity (null) bitmap.
% The Apache Arrow specification defines 0 (false) to represent an
% invalid (null) array entry and 1 (true) to represent a valid
% (non-null) array entry.
for ii = 1:length(variables)
    if ~all(variables(ii).Valid)
        switch variables(ii).Type
            case {'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64'}
                % MATLAB does not support missing values for integer types, so
                % cast to double and set missing values to NaN in this case.
                variables(ii).Data = double(variables(ii).Data);
        end

        % Set invalid (null) entries to the appropriate MATLAB missing value using
        % logical indexing.
        variables(ii).Data(~variables(ii).Valid) = missing;
    end
end

% Construct a MATLAB table from the Feather file data.
t = table(variables.Data, 'VariableNames', cellstr(variableNames));

% Store original Feather table column names in the table.Properties.VariableDescriptions
% property if they were modified to be valid MATLAB table variable names.
if ~isempty(variableDescriptions)
    t.Properties.VariableDescriptions = cellstr(variableDescriptions);
end

% Set the Description property of the table based on the Feather file
% description.
t.Properties.Description = metadata.Description;

end
