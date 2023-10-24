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

# Guideline for Writing Tests for MATLAB Interface for Apache Arrow

## Overview  

This document is aimed at providing guidelines on testing when people contribute to the [`mathworks/arrow`](https://github.com/mathworks/arrow) repository.  

## Test File Location and Organization  

All tests for MATLAB interface are put under `matlab/test`. To help easily locate related tests for each source file, test organization follows the following two rules:  

- Test directory maps source directory. For example,  

    | Test Directory            | Corresponding Source Directory     |  
    |---------------------------|------------------------------------|  
    | test/**arrow/array/**     | src/matlab/**+arrow/+array/**      |  
    | test/**arrow/internal/**  | src/matlab/**+arrow/+internal/**   |  
    | test/**arrow/io/**        | src/matlab/**+arrow/+io/**         |  
    | test/**arrow/tabular/**   | src/matlab/**+arrow/+tabular/**    |  
    | test/**arrow/type/**      | src/matlab/**+arrow/+type/**       |  
- One test for one source file. For example `test/arrow/array/tArray.m` is the test file for `src/matlab/+arrow/+array/Array.m`.
## When tests are run  

When pushing changes to the [`mathworks/arrow`](https://github.com/mathworks/arrow) repository, GitHub Actions is used to automatically trigger tests under `matlab/test` to run. Currently, all tests under `matlab/test` are configured to run. In the future when there are a lot of tests, we might consider doing source to test mapping and only triggering related tests to run.  

## How to Write Tests  

## How to Run Tests Locally on Your Own Machine  

### Prerequisites  

The tests are written by using MATLAB testing framework [`matlab.unittest.TestCase class`](https://www.mathworks.com/help/matlab/ref/matlab.unittest.testcase-class.html). The run the tests, the following software must be installed on your machine:  

1. [MATLAB](https://www.mathworks.com/products/get-matlab.html)
2. [MATLAB Interface to Apache Arrow](https://github.com/mathworks/arrow/tree/main/matlab)  

### Run tests in MATLAB  

To run the MATLAB tests, start MATLAB and then cd to the test directory where test files of interest reside.  Call the runtests command to run your tests:  

```matlab
>> runtests(testFileName) % For example runtests("tArray.m")
```

To learn more about `runtests`, please check [the document of `runtests`](https://www.mathworks.com/help/matlab/ref/runtests.html#btzwrop-tests).  



## Test Case Design  

## Code Coverage