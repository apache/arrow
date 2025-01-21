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

    arguments
        filename(1, 1) string {mustBeNonmissing, mustBeNonzeroLengthText}
    end

    typesToCast = [arrow.type.ID.UInt8, ...
                   arrow.type.ID.UInt16, ...
                   arrow.type.ID.UInt32, ...
                   arrow.type.ID.UInt64, ...
                   arrow.type.ID.Int8, ...
                   arrow.type.ID.Int16, ...
                   arrow.type.ID.Int32, ...
                   arrow.type.ID.Int64, ...
                   arrow.type.ID.Boolean];

    reader = arrow.internal.io.feather.Reader(filename);
    recordBatch = reader.read();

    % Convert RecordBatch to a MATLAB table.
    t = table(recordBatch);

    % Cast integer and boolean columns containing null values
    % to double. Substitute null values with NaN.
    for ii = 1:recordBatch.NumColumns
        array = recordBatch.column(ii);
        type = array.Type.ID;
        if any(type == typesToCast) && any(~array.Valid)
            % Cast to double.
            t.(ii) = double(t.(ii));
            % Substitute null values with NaN.
            t{~array.Valid, ii} = NaN;
        end
    end

    % Store original Feather table column names in the table.Properties.VariableDescriptions
    % property if they were modified to be valid MATLAB table variable names.
    modifiedColumnNameIndices = t.Properties.VariableNames ~= recordBatch.ColumnNames;
    if any(modifiedColumnNameIndices)
        originalColumnNames = recordBatch.ColumnNames(modifiedColumnNameIndices);
        t.Properties.VariableDescriptions(modifiedColumnNameIndices) = compose("Original variable name: '%s'", originalColumnNames);
    end

end
