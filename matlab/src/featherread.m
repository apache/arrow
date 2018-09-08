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

% Preallocate a cell array for the case in which
% the table VariableDescriptions property needs to be modified.
variableDescriptions = cell(1, numel(variables));

% Iterate over each table variable, handling null entries and invalid
% variable names appropriately.
for ii = 1:length(variables)
    if any(variables(ii).Nulls)
        switch variables(ii).Type
            case {'uint8', 'uint16', 'uint32', 'uint64', 'int8', 'int16', 'int32', 'int64'}
                % MATLAB does not support missing values for integer types, so
                % cast to double and set missing values to NaN in this case.
                variables(ii).Data = double(variables(ii).Data);
        end

        % Set null entries to the appropriate MATLAB missing value using
        % logical indexing.
        variables(ii).Data(variables(ii).Nulls) = missing;
    end

    % Store invalid variable names in the VariableDescriptons
    % property, and convert any invalid variable names into valid variable
    % names.
    setVariableDescriptions = false;
    if ~isvarname(variables(ii).Name)
        variableDescriptions{ii} = sprintf('Original variable name: ''%s''', variables(ii).Name);
        setVariableDescriptions = true;
    else
        variableDescriptions{ii} = '';
    end
end

% Construct a MATLAB table from the Feather file data.
t = table(variables.Data, 'VariableNames', matlab.lang.makeValidName({variables.Name}));

% Store original variable names in the VariableDescriptions property
% if they were modified to be valid MATLAB table variable names.
if setVariableDescriptions
    t.Properties.VariableDescriptions = variableDescriptions;
end
% Set the Description property of the table based on the Feather file
% description.
t.Properties.Description = metadata.Description;

end
