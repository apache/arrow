---
layout: post
title: "Improved JAVA Vector APIs"
excerpt: "This post describes the recent improvements in JAVA Vector code"
date: 2017-12-13 12:50:00
author: Siddharth Teotia
categories: [application]
---

<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->


This post gives insight into the major improvements in the JAVA implementation
of vectors.

## Design Goals

1. Improved Maintainability and Extensibility.
2. Improved heap usage.
3. No performance overhead on hot code paths.

## Background

**Improved Maintainability and Extensibility** 

We use templates in several places for compile time JAVA code generation for
different vector classes, readers, writers etc. Templates are helpful as the
developers don't have to write a lot of duplicate code. 

However, we realized that over a period of time some specific JAVA 
templates became extremely complex with giant if-else blocks, poor code indentation
and documentation. All this impacted the ability to easily extend these templates 
for adding new functionality or improving the existing infrastructure.

So we evaluated the usage of templates for compile time code generation and
decided not to use comlplex templates in some places by writing small amount of 
duplicate code which is elegant, well documented and extensible.

**Improved Heap Usage**

We did extensive memory analysis downstream in Dremio where Arrow is used
heavily for in-memory query execution on columnar data. The general conclusion
was that Arrow JAVA Vectors have non-negligible heap overhead and volume of 
objects was too high. There were places in code where we were creating objects
unnecessarily and using structures that could be substituted with better
alternatives.

**No performance overhead on hot code paths**

JAVA Vectors used delegation and abstraction heavily throughout the object 
hierarchy. The performance critical get/set methods of vectors went through
a chain of function calls back and forth between different objects before 
doing meaningful work. We also evaluated the usage of branches in vector
APIs and reimplemented some of them by avoiding branches completely.

We took inspiration from how the JAVA memory code in ArrowBuf works. For
all the performance critical methods, ArrowBuf bypasses all the netty object 
hierarchy, grabs the target virtual address and directly interacts with 
the memory.

There were cases where branches could be avoided all together.

In case of Nullable vectors, we were doing multiple checks to confirm if
the value at a given position in the vector is NULL or not. 

## Our Implementation Approach

- For scalars, the inheritance tree was simplified by writing different
abstract base classes for fixed and variable width scalars. 
- The base classes contained all the common functionality across different
types.
- The individual subclasses implemented type specific APIs for fixed and
variable width scalar vectors. 
- For the performance critical methods, all the work is done either in
the vector class or corresponding ArrowBuf. There is no delegation to any
internal object.
- The mutator and accessor based access to vector APIs is removed. These 
objects led to unnecessary heap overhead and complicated the use of APIs.
- Both scalar and complex vectors directly interact with underlying buffers
that manage the offsets, data and validity. Earlier we were creating different
inner vectors for each vector and delegating all the functionality to inner
vectors. This introduced a lot of bugs in memory management, excessive heap 
overhead and performance penalty due to chain of delegations.
- We reduced the number of vector classes by removing non-nullable vectors.
In the new implementation, all vectors in JAVA are nullable in nature.

## Performance Testing

We did performance testing downstream in Dremio and saw a 5% average improvement
in Tpch queries. 



