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

## Apache Arrow C++ Compute Functions

This submodule contains analytical functions that process primarily Arrow
columnar data; some functions can process scalar or Arrow-based array
inputs. These are intended for use inside query engines, data frame libraries,
etc.

Many functions have SQL-like semantics in that they perform elementwise or
scalar operations on whole arrays at a time. Other functions are not SQL-like
and compute results that may be a different length or whose results depend on
the order of the values.

Some basic terminology:

* We use the term "function" to refer to particular general operation that may
  have many different implementations corresponding to different combinations
  of types or function behavior options.
* We call a specific implementation of a function a "kernel". When executing a
  function on inputs, we must first select a suitable kernel (kernel selection
  is called "dispatching") corresponding to the value types of the inputs
* Functions along with their kernel implementations are collected in a
  "function registry". Given a function name and argument types, we can look up
  that function and dispatch to a compatible kernel.

Types of functions

* Scalar functions: elementwise functions that perform scalar operations in a
  vectorized manner. These functions are generally valid for SQL-like
  context. These are called "scalar" in that the functions executed consider
  each value in an array independently, and the output array or arrays have the
  same length as the input arrays. The result for each array cell is generally
  independent of its position in the array.
* Vector functions, which produce a result whose output is generally dependent
  on the entire contents of the input arrays. These functions **are generally
  not valid** for SQL-like processing because the output size may be different
  than the input size, and the result may change based on the order of the
  values in the array. This includes things like array subselection, sorting,
  hashing, and more.
* Scalar aggregate functions of which can be used in a SQL-like context