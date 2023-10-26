<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Testing Guidelines for the MATLAB Interface to Apache Arrow  

## Overview  

The goal of this document is to provide helpful guidelines for testing functionality within the [`matlab` directory](https://github.com/apache/arrow/tree/main/matlab) of the [`apache/arrow`](https://github.com/apache/arrow) repository.  

## Prerequisites  

MATLAB interface has its own tests to ensure its quality. To run those tests locally on your machine, the following software must be installed on your machine:  

1. [MATLAB](https://www.mathworks.com/products/get-matlab.html)
2. [MATLAB Interface to Apache Arrow](https://github.com/mathworks/arrow/tree/main/matlab)

## General Guidelines  

When adding new tests, it is recommended to, at a minimum, ensure that real-world workflows work as expected.  

### MATLAB Class-Based Unit Testing Framework  

All tests for the MATLAB interface should use the [MATLAB Class-Based Unit Testing Framework](https://www.mathworks.com/help/matlab/class-based-unit-tests.html) (i.e. they should use [`matlab.unittest.TestCase`](https://www.mathworks.com/help/matlab/ref/matlab.unittest.testcase-class.html)).  

## Code Coverage Goals  

When making changes to the MATLAB interface, please do your best to add tests to cover all changed lines, conditions, and decisions.  

Before making a pull request, please check the code coverage for any changed code. If possible, it can be helpful to explicitly comment on the code coverage in your pull request description.  

Although we strive for high code coverage, it is understood that some code cannot be reasonably tested (e.g. an "un-reachable" branch in a `switch` condition on an enumeration value).

### How to Check Code Coverage  

To generate a MATLAB code coverage report, the [`ReportCoverageFor`](https://www.mathworks.com/help/matlab/ref/runtests.html#mw_764c9db7-6823-439f-a77d-7fd25a03d20e) name-value pair argument can be supplied to the [`runtests`](https://www.mathworks.com/help/matlab/ref/runtests.html) command. Before generating the code coverage report, remember to add your source file directory to path. Here are the commands.

```matlab  
>> addpath( genpath(<your local arrow/matlab>) ) % Include subdirectories
>> runtests(testFilePath/testFolderPath, 'ReportCoverageFor', sourceFilePath/sourceFolderPath, 'IncludeSubfolders', true/false);  
```

Below is an example of running all tests under `matlab/test` and getting the MATLAB code coverage report for all files under `matlab/src/matlab`.

```matlab  
>> addpath(genpath("C:\TryCodeCoverage\arrow\matlab"))
>> runtests('C:\TryCodeCoverage\arrow\matlab\test', 'ReportCoverageFor', 'C:\TryCodeCoverage\arrow\matlab\src\matlab\', 'IncludeSubfolders', true);
```

## Test Organization  

All tests for the MATLAB interface are located under the `matlab/test` directory.  

To make it easy to find the test files which correspond to specific source files, the MATLAB interface tests are organized using the following rules:  

- Source and test directories follow an (approximately) "parallel" structure. For example, the test directory [`test/arrow/array`](https://github.com/apache/arrow/tree/main/matlab/test/arrow/array) contains tests for the source directory [`src/matlab/+arrow/+array`](https://github.com/apache/arrow/tree/main/matlab/src/matlab/%2Barrow/%2Barray).  
- One test file maps to one source file. For example, [`test/arrow/array/tArray.m`](https://github.com/apache/arrow/blob/main/matlab/test/arrow/array/tArray.m) is the test file for [`src/matlab/+arrow/+array/Array.m`](https://github.com/apache/arrow/blob/main/matlab/src/matlab/%2Barrow/%2Barray/Array.m).  
- **Note**: In certain scenarios, it can make sense to diverge from these rules. For example, if a particular class is very complex and contains a lot of divergent functionality (which we generally try to avoid), we might choose to split the testing into several "focused" test files (e.g. one for testing the class display, one for testing the properties, and one for testing the methods).  

## Continuous Integration (CI) Workflows  

The Apache Arrow project uses [GitHub Actions](https://github.com/features/actions) as its primary [Continuous Integration (CI)](https://en.wikipedia.org/wiki/Continuous_integration) platform.  

Creating a pull request that changes code in the MATLAB interface will automatically trigger [MATLAB CI Workflows](https://github.com/apache/arrow/actions/workflows/matlab.yml) to be run. These CI workflows will run all tests located under the `matlab/test` directory.  

Reviewers will generally expect the MATLAB CI Workflows to be passing successfully before they will consider merging a pull request.  

If you are having trouble understanding CI failures, you can always ask a reviewer or another community member for help.  

## Writing Tests  

Tests for MATLAB interface should use the MATLAB testing framework [`matlab.unittest.TestCase class`](https://www.mathworks.com/help/matlab/ref/matlab.unittest.testcase-class.html). Here is a simple example.  

```matlab
classdef tStringArray < matlab.unittest.TestCase
    methods(Test)
        function TestBasicStringArray(testCase)
            % Verify that an `arrow.array.StringArray` can be created from
            % a basic MATLAB `string` array using the `arrow.array` gateway
            % construction function.

            % Create a basic MATLAB array.
            matlabArray = ["A" ,"B", "C"];
            % Create an `arrow.array.StringArray` from the MATLAB `string`
            % array by using the `arrow.array` gateway construction function.
            arrowArray = arrow.array(matlabArray);
            % Verify the class of `arrowArray` is `arrow.array.StringArray`.
            testCase.verifyEqual(string(class(arrowArray)), "arrow.array.StringArray");
            % Verify the value of `arrowArray`
            testCase.verifyEqual(arrowArray.toMATLAB,["A";"B";"C"]);
        end
    end
end
```

More test examples can be found in the `matlab/test` directory.  

### Testing Best Practices  

- Use descriptive names for your test cases.
- Focus on testing one software "behavior" in each test case.
- Test with both "expected" and "unexpected" inputs.
- Add a comment at the beginning of each test case which describes what the test case is verifying.
- Treat test code like any other code (i.e. use clear variable names, write helper functions, make use of abstraction, etc.)
- Follow existing patterns when adding new test cases to an existing test class.

## Running Tests Locally   

To run the MATLAB tests, start MATLAB and then cd to the test directory where test files of interest reside.  Call the `runtests` command to run your tests:  

```matlab
% To run a single test file
>> runtests(testFileName) % For example runtests("tArray.m")

% To run tests under a test directory
>> runtests(testFolderName, IncludeSubfolders=true) % For example runtests('matlab\test',IncludeSubfolders=true)
```

To learn more about `runtests`, please check [the documentation of `runtests`](https://www.mathworks.com/help/matlab/ref/runtests.html).  

## Tips  

### Generate MATLAB Code Coverage Report  

It might happen that caching or other issue causes zero coverage because the test ends up executing some other source file. To avoid such problem, please first set a breakpoint in your source file and run the tests. This step is to make sure that your source file is executed by the tests. After this step, the command `runtests(___, "ReportCoverageFor", ___)` will work well.
