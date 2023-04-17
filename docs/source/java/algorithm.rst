.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

Java Algorithms
===============

Arrow's Java library provides algorithms for some commonly-used
functionalities. The algorithms are provided in the ``org.apache.arrow.algorithm``
package of the ``algorithm`` module. 

Comparing Vector Elements
-------------------------

Comparing vector elements is the basic for many algorithms. Vector 
elements can be compared in one of the two ways:

1. **Equality comparison**: there are two possible results for this type of comparisons: ``equal`` and ``unequal``.
Currently, this type of comparison is supported through the ``org.apache.arrow.vector.compare.VectorValueEqualizer``
interface.

2. **Ordering comparison**: there are three possible results for this type of comparisons: ``less than``, ``equal to``
and ``greater than``. This comparison is supported by the abstract class ``org.apache.arrow.algorithm.sort.VectorValueComparator``.

We provide default implementations to compare vector elements. However, users can also define ways
for customized comparisons. 

Vector Element Search
---------------------

A search algorithm tries to find a particular value in a vector. When successful, a vector index is 
returned; otherwise, a ``-1`` is returned. The following search algorithms are provided:

1. **Linear search**: this algorithm simply traverses the vector from the beginning, until a match is 
found, or the end of the vector is reached. So it takes ``O(n)`` time, where ``n`` is the number of elements
in the vector.  This algorithm is implemented in ``org.apache.arrow.algorithm.search.VectorSearcher#linearSearch``.

2. **Binary search**: this represents a more efficient search algorithm, as it runs in ``O(log(n))`` time. 
However, it is only applicable to sorted vectors. To get a sorted vector,
one can use one of our sorting algorithms, which will be discussed in the next section. This algorithm
is implemented in ``org.apache.arrow.algorithm.search.VectorSearcher#binarySearch``.

3. **Parallel search**: when the vector is large, it takes a long time to traverse the elements to search
for a value. To make this process faster, one can split the vector into multiple partitions, and perform the 
search for each partition in parallel. This is supported by ``org.apache.arrow.algorithm.search.ParallelSearcher``.

4. **Range search**: for many scenarios, there can be multiple matching values in the vector. 
If the vector is sorted, the matching values reside in a contiguous region in the vector. The
range search algorithm tries to find the upper/lower bound of the region in ``O(log(n))`` time. 
An implementation is provided in ``org.apache.arrow.algorithm.search.VectorRangeSearcher``.

Vector Sorting
--------------

Given a vector, a sorting algorithm turns it into a sorted one. The sorting criteria must
be specified by some ordering comparison operation. The sorting algorithms can be
classified into the following categories:

1. **In-place sorter**: an in-place sorter performs the sorting by manipulating the original
vector, without creating any new vector. So it just returns the original vector after the sorting operations.
Currently, we have ``org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter`` for in-place
sorting in ``O(nlog(n))`` time. As the name suggests, it only supports fixed width vectors. 

2. **Out-of-place sorter**: an out-of-place sorter does not mutate the original vector. Instead,
it copies vector elements to a new vector in sorted order, and returns the new vector.
We have ``org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter.FixedWidthOutOfPlaceVectorSorter`` 
and ``org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter.VariableWidthOutOfPlaceVectorSorter``
for fixed width and variable width vectors, respectively. Both algorithms run in ``O(nlog(n))`` time. 

3. **Index sorter**: this sorter does not actually sort the vector. Instead, it returns an integer
vector, which correspond to indices of vector elements in sorted order. With the index vector, one can
easily construct a sorted vector. In addition, some other tasks can be easily achieved, like finding the ``k``th
smallest value in the vector. Index sorting is supported by ``org.apache.arrow.algorithm.sort.IndexSorter``, 
which runs in ``O(nlog(n))`` time. It is applicable to vectors of any type. 

Other Algorithms
----------------

Other algorithms include vector deduplication, dictionary encoding, etc., in the ``algorithm`` module.
