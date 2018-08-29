classdef tfeather < matlab.unittest.TestCase
    % Tests for MATLAB featherread and featherwrite.

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
    
    methods(TestClassSetup)
        
        function addFeatherFunctionsToMATLABPath(testcase)
            import matlab.unittest.fixtures.PathFixture
            % Add featherread and featherwrite to the MATLAB path.
            testcase.applyFixture(PathFixture(fullfile('..', 'src')));
            % featherreadmex must be on the MATLAB path.
            testcase.assertTrue(~isempty(which('featherreadmex')), ...
                '''featherreadmex'' must be on the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
            % featherwritemex must be on the MATLAB path.
            testcase.assertTrue(~isempty(which('featherwritemex')), ...
                '''featherwritemex'' must be on to the MATLAB path. Use ''addpath'' to add folders to the MATLAB path.');
        end
        
    end
    
    methods(Test)

        function NumericDatatypesNoNulls(testcase)
            actTable=generatetable;
            expectedTable=featherRoundtrip(actTable);
            testcase.verifyEqual(actTable,expectedTable);
        end

        function NumericDatatypesWithNaNRow(testcase)
            t = generatetable;
            
            variableNames = {'single', ...
                'double', ...
                'int8', ...
                'int16', ...
                'int32', ...
                'int64', ...
                'uint8', ...
                'uint16', ...
                'uint32', ...
                'uint64'};
            variableTypes = repmat({'double'},10,1)';
            numRows = 1;
            numVariables = 10;
            
            addRow = table('Size', [numRows, numVariables], 'VariableTypes', variableTypes, 'VariableNames', variableNames);
            addRow(1,:)={NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN, NaN};
            actTable=[t;addRow];
            expectedTable=featherRoundtrip(actTable);
            testcase.verifyEqual(actTable,expectedTable);
        end

        function NumericDatatypesWithNaNColumns(testcase)
            actTable=generatetable;
            actTable.double=[NaN;NaN];
            actTable.int64=[NaN;NaN];
            
            expectedTable=featherRoundtrip(actTable);
            testcase.verifyEqual(actTable,expectedTable);
        end
        
        function NumericDatatypesWithExpInfSciNotation(testcase)
            actTable=generatetable;
            actTable.single(2)=1.0418e+06;
            
            actTable.double(1)=Inf;
            actTable.double(2)=exp(9);
            
            actTable.int64(2)=1.0418e+03;
           
            expectedTable=featherRoundtrip(actTable);
            testcase.verifyEqual(actTable,expectedTable);
        end
        
        function IgnoreRowVarNames(testcase)
            actTable=generatetable;
            time={'day1','day2'};
            actTable.Properties.RowNames=time;
            expectedTable=featherRoundtrip(actTable);
            actTable=generatetable;
            testcase.verifyEqual(actTable,expectedTable);
        end
        
        function UnsupportedFileFormat(testcase)
            actTable=generatetable;
            filename=[tempname,'.txt'];
            featherwrite(filename,actTable);
            expectedTable=featherread(filename);
            testcase.verifyEqual(actTable,expectedTable);
        end
        
        function EmptyTable(testcase)
            actTable=table;
            expectedTable=featherRoundtrip(actTable);
            testcase.verifyEqual(actTable,expectedTable);
        end

        function zeroByNTable(testcase)
            actTable=generatetable;
            actTable([1,2],:)=[];
            expectedTable=featherRoundtrip(actTable);
            testcase.verifyEqual(actTable,expectedTable);
        end

        % %%%%%%%%%%%%%%%%%%%
        % Negative test cases
        % %%%%%%%%%%%%%%%%%%%
      
        function ErrorIfNotAFeatherFile(testcase)
            t=generatetable;
            testcase.verifyError(@() featherread(t), 'MATLAB:arrow:InvalidFilenameDatatype');
        end

        function ErrorIfUnableToOpenFile(testcase)
            filename = 'nonexistent.feather';

            testcase.verifyError(@() featherread(filename), 'MATLAB:arrow:UnableToOpenFile');
        end

        function ErrorIfCorruptedFeatherFile(testcase)
            t=generatetable;
            filename=[tempname,'.feather'];
            featherwrite(filename,t);
            
            fileID=fopen(filename,'w');
            fwrite(fileID,[1;5]);
            fclose(fileID);
            
            testcase.verifyError(@() featherread(filename), 'MATLAB:arrow:status:Invalid');
        end
        function ErrorIfInvalidFilenameDatatype(testcase)
            t=generatetable;
            filename=[tempname,'.feather'];
            featherwrite(filename,t);

            testcase.verifyError(@() featherread({filename}), 'MATLAB:arrow:InvalidFilenameDatatype');
        end

        function ErrorIfTooManyInputs(testcase)
            t=generatetable;
            filename=[tempname,'.feather'];
            featherwrite(filename,t);

            testcase.verifyError(@() featherread(filename, 'SomeValue'), 'MATLAB:TooManyInputs');
        end

        function ErrorIfTooFewInputs(testcase)
            testcase.verifyError(@() featherread(), 'MATLAB:narginchk:notEnoughInputs');
        end
        
        function ErrorIfMultiColVarExist(testcase)
            Age = [38;43;38;40;49];
            Smoker = logical([1;0;1;0;1]);
            Height = [71;69;64;67;64];
            Weight = [176;163;131;133;119];
            BloodPressure = [124 93; 109 77; 125 83; 117 75; 122 80];
            
            t = table(Age,Smoker,Height,Weight,BloodPressure);
            multiColVar=@()featherwrite([tempname,'.feather'],t);
            testcase.verifyError(multiColVar, 'MATLAB:arrow:UnsupportedVariableType');
        end
        
        function unsupportedDatatypes(testcase)
            actTable=generatetable;
            time=[calendarDuration(1,7,9);calendarDuration(2,1,1)];
            actTable=addvars(actTable,time);
            tableWithUnsupportedDatatypes=@()featherwrite([tempname,'.feather'],actTable);
            testcase.verifyError(tableWithUnsupportedDatatypes,'MATLAB:arrow:UnsupportedVariableType');
        end
        
        function ErrorForComplexNum(testcase)
            actTable=generatetable;
            actTable.single(1)=1.0418+2i;
           
            actTable.double(2)=exp(9)+5i;
            
            actTable.int64(2)=1.0418e+03;
           
            expectedTable=featherRoundtrip(actTable);
            testcase.verifyNotEqual(actTable,expectedTable);
        end        
    end
end

%%%Roundtrip%%%%%%%%%%%%
function expectedTable=featherRoundtrip(t)
    % Generate a temporary file path in your system's temporary folder
    filename=[tempname,'.feather'];
    % Call featherwrite() and featherread()
    featherwrite(filename,t);
    expectedTable=featherread(filename);
end
    
function t=generatetable
    variableNames = {'single', ...
        'double', ...
        'int8', ...
        'int16', ...
        'int32', ...
        'int64', ...
        'uint8', ...
        'uint16', ...
        'uint32', ...
        'uint64'};
    variableTypes = {'single', ...
        'double', ...
        'int8', ...
        'int16', ...
        'int32', ...
        'int64', ...
        'uint8', ...
        'uint16', ...
        'uint32', ...
        'uint64'};
    numRows = 2;
    numVariables = 10;

    t = table('Size', [numRows, numVariables], 'VariableTypes', variableTypes, 'VariableNames', variableNames);
    t(1, :) = {1/3, 2/3, 1, 2, 3, 4,  9, 10, 11, 12};
    t(2, :) = {4,   5,   5, 6, 7, 8, 13, 14, 15, 16};
end
