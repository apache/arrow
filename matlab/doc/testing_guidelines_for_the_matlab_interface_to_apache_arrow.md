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

This document is aimed at providing guidelines on testing of the `matlab` directory in the [`apache/arrow`](https://github.com/apache/arrow) repository.  

## General Guideline  

When designing any test case, please try to design it at the MATLAB interface level such that the tests are testing real use cases. Tests for MATLAB interface should use the MATLAB testing framework [`matlab.unittest.TestCase class`](https://www.mathworks.com/help/matlab/ref/matlab.unittest.testcase-class.html).

If the source code cannot be covered by any test at the MATLAB interface level, consider adding c++ unit tests by using GoogleTest framework.

## Code Coverage Goals  

When making change to the `matlab` directory in the [`apache/arrow`](https://github.com/apache/arrow) repository, please try to add tests to cover all changed lines, conditions and decisions. It is understandable that some code cannot be hit due to defensive purpose or other reasons. When reviewer reviews code change, they'll check the code coverage for the changed code.  

### How to Check Code Coverage  

The Name-Value input argument [`ReportCoverageFor`](https://www.mathworks.com/help/matlab/ref/runtests.html#mw_764c9db7-6823-439f-a77d-7fd25a03d20e) to `runtests` is used to generate code coverage report.  


```matlab  
>> addpath( <your local arrow/matlab> )
>> runtests(testFilePath/testFolderPath, 'ReportCoverageFor', sourceFilePath/sourceFolderPath, 'IncludeSubfolders', true/false);  

% Example
>> runtests('C:\TryCodeCoverage\arrow\matlab\test', 'ReportCoverageFor', 'C:\TryCodeCoverage\arrow\matlab\src\matlab\', 'IncludeSubfolders', true);
```

It might happen that caching or shadowing issue causes zero coverage because the test ends up executing some other source file. To avoid such problem, please first set a breakpoint in your source file and run the tests. This step is to make sure that your source file is executed by the tests. After this step, the command `runtests(___, "ReportCoverageFor", ___)` will work well.

## Test File Location and Organization  

All tests for MATLAB interface are put under `matlab/test`. To help easily locate related tests for each source file, test organization follows the following two rules:  

- Test directory maps source directory. For example, the test directory `test/arrow/array/` contains all tests for the source directory `src/matlab/+arrow/+array/`.  
- One test for one source file. For example, `test/arrow/array/tArray.m` is the test file for `src/matlab/+arrow/+array/Array.m`.  

## When tests are run  

When pushing changes to the `matlab` directory, GitHub Actions is used to automatically trigger tests under `matlab/test` to run. Currently, all tests under `matlab/test` are configured to run. In the future when the run time of running tests becomes a concern, we will consider doing source to test mapping and only triggering related tests to run.  

## How to Write Tests  

Tests for MATLAB interface should use the MATLAB testing framework [`matlab.unittest.TestCase class`](https://www.mathworks.com/help/matlab/ref/matlab.unittest.testcase-class.html). Here is a simple example.  

```matlab
classdef tArray < matlab.unittest.TestCase
    methods(Test)
        function InferNullsTrue(testCase)
            matlabArray = [1 2 NaN 3];
            arrowArray = arrow.array(matlabArray, InferNulls=true);
            testCase.verifyEqual(arrowArray.Valid, [true; true; false; true]);
        end
    end
end
```

More test examples can be found in the directory `matlab/test`.  

## How to Run Tests Locally on Your Own Machine  

### Prerequisites  

The tests are written by using MATLAB testing framework [`matlab.unittest.TestCase class`](https://www.mathworks.com/help/matlab/ref/matlab.unittest.testcase-class.html). To run the tests on your machine, the following software must be installed on your machine:  

1. [MATLAB](https://www.mathworks.com/products/get-matlab.html)
2. [MATLAB Interface to Apache Arrow](https://github.com/mathworks/arrow/tree/main/matlab)  

### Run tests in MATLAB  

To run the MATLAB tests, start MATLAB and then cd to the test directory where test files of interest reside.  Call the `runtests` command to run your tests:  

```matlab
>> runtests(testFileName) % For example runtests("tArray.m")
```

To learn more about `runtests`, please check [the document of `runtests`](https://www.mathworks.com/help/matlab/ref/runtests.html).  
