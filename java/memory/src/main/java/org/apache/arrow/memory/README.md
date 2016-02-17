<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
# Memory: Allocation, Accounting and Management
 
The memory management package contains all the memory allocation related items that Arrow uses to manage memory.


## Key Components
Memory management can be broken into the following main components:

- Memory chunk allocation and fragmentation management
  - `PooledByteBufAllocatorL` - A LittleEndian clone of Netty's jemalloc implementation
  - `UnsafeDirectLittleEndian` - A base level memory access interface
  - `LargeBuffer` - A buffer backing implementation used when working with data larger than one Netty chunk (default to 16mb)
- Memory limits & Accounting
  - `Accountant` - A nestable class of lockfree memory accountors.
- Application-level memory allocation
  - `BufferAllocator` - The public interface application users should be leveraging
  - `BaseAllocator` - The base implementation of memory allocation, contains the meat of our the Arrow allocator implementation
  - `RootAllocator` - The root allocator. Typically only one created for a JVM
  - `ChildAllocator` - A child allocator that derives from the root allocator
- Buffer ownership and transfer capabilities
  - `AllocationManager` - Responsible for managing the relationship between multiple allocators and a single chunk of memory
  - `BufferLedger` - Responsible for allowing maintaining the relationship between an `AllocationManager`, a `BufferAllocator` and one or more individual `ArrowBuf`s 
- Memory access
  - `ArrowBuf` - The facade for interacting directly with a chunk of memory.
 

## Memory Management Overview
Arrow's memory model is based on the following basic concepts:

 - Memory can be allocated up to some limit. That limit could be a real limit (OS/JVM) or a locally imposed limit.
 - Allocation operates in two phases: accounting then actual allocation. Allocation could fail at either point.
 - Allocation failure should be recoverable. In all cases, the Allocator infrastructure should expose memory allocation failures (OS or internal limit-based) as `OutOfMemoryException`s.
 - Any allocator can reserve memory when created. This memory shall be held such that this allocator will always be able to allocate that amount of memory.
 - A particular application component should work to use a local allocator to understand local memory usage and better debug memory leaks.
 - The same physical memory can be shared by multiple allocators and the allocator must provide an accounting paradigm for this purpose.

## Allocator Trees

Arrow provides a tree-based model for memory allocation. The RootAllocator is created first, then all allocators are created as children of that allocator. The RootAllocator is responsible for being the master bookeeper for memory allocations. All other allocators are created as children of this tree. Each allocator can first determine whether it has enough local memory to satisfy a particular request. If not, the allocator can ask its parent for an additional memory allocation.

## Reserving Memory

Arrow provides two different ways to reserve memory:

  - BufferAllocator accounting reservations: 
      When a new allocator (other than the `RootAllocator`) is initialized, it can set aside memory that it will keep locally for its lifetime. This is memory that will never be released back to its parent allocator until the allocator is closed.
  - `AllocationReservation` via BufferAllocator.newReservation(): Allows a short-term preallocation strategy so that a particular subsystem can ensure future memory is available to support a particular request.
  
## Memory Ownership, Reference Counts and Sharing
Many BufferAllocators can reference the same piece of memory at the same time. The most common situation for this is in the case of a Broadcast Join: in this situation many downstream operators in the same Arrowbit will receive the same physical memory. Each of these operators will be operating within its own Allocator context. We therefore have multiple allocators all pointing at the same physical memory. It is the AllocationManager's responsibility to ensure that in this situation, that all memory is accurately accounted for from the Root's perspective and also to ensure that the memory is correctly released once all BufferAllocators have stopped using that memory.

For simplicity of accounting, we treat that memory as being used by one of the BufferAllocators associated with the memory. When that allocator releases its claim on that memory, the memory ownership is then moved to another BufferLedger belonging to the same AllocationManager. Note that because a ArrowBuf.release() is what actually causes memory ownership transfer to occur, we always precede with ownership transfer (even if that violates an allocator limit). It is the responsibility of the application owning a particular allocator to frequently confirm whether the allocator is over its memory limit (BufferAllocator.isOverLimit()) and if so, attempt to aggresively release memory to ameliorate the situation.

All ArrowBufs (direct or sliced) related to a single BufferLedger/BufferAllocator combination share the same reference count and either all will be valid or all will be invalid.

## Object Hierarchy

There are two main ways that someone can look at the object hierarchy for Arrow's memory management scheme. The first is a memory based perspective as below:

### Memory Perspective
<pre>
+ AllocationManager
|
|-- UnsignedDirectLittleEndian (One per AllocationManager)
|
|-+ BufferLedger 1 ==> Allocator A (owning)
| ` - ArrowBuf 1
|-+ BufferLedger 2 ==> Allocator B (non-owning)
| ` - ArrowBuf 2
|-+ BufferLedger 3 ==> Allocator C (non-owning)
  | - ArrowBuf 3
  | - ArrowBuf 4
  ` - ArrowBuf 5
</pre>

In this picture, a piece of memory is owned by an allocator manager. An allocator manager is responsible for that piece of memory no matter which allocator(s) it is working with. An allocator manager will have relationships with a piece of raw memory (via its reference to UnsignedDirectLittleEndian) as well as references to each BufferAllocator it has a relationship to. 

### Allocator Perspective
<pre>
+ RootAllocator
|-+ ChildAllocator 1
| | - ChildAllocator 1.1
| ` ...
|
|-+ ChildAllocator 2
|-+ ChildAllocator 3
| |
| |-+ BufferLedger 1 ==> AllocationManager 1 (owning) ==> UDLE
| | `- ArrowBuf 1
| `-+ BufferLedger 2 ==> AllocationManager 2 (non-owning)==> UDLE
| 	`- ArrowBuf 2
|
|-+ BufferLedger 3 ==> AllocationManager 1 (non-owning)==> UDLE
| ` - ArrowBuf 3
|-+ BufferLedger 4 ==> AllocationManager 2 (owning) ==> UDLE
  | - ArrowBuf 4
  | - ArrowBuf 5
  ` - ArrowBuf 6
</pre>

In this picture, a RootAllocator owns three ChildAllocators. The first ChildAllocator (ChildAllocator 1) owns a subsequent ChildAllocator. ChildAllocator has two BufferLedgers/AllocationManager references. Coincidentally, each of these AllocationManager's is also associated with the RootAllocator. In this case, one of the these AllocationManagers is owned by ChildAllocator 3 (AllocationManager 1) while the other AllocationManager (AllocationManager 2) is owned/accounted for by the RootAllocator. Note that in this scenario, ArrowBuf 1 is sharing the underlying memory as ArrowBuf 3. However the subset of that memory (e.g. through slicing) might be different. Also note that ArrowBuf 2 and ArrowBuf 4, 5 and 6 are also sharing the same underlying memory. Also note that ArrowBuf 4, 5 and 6 all share the same reference count and fate.

## Debugging Issues
The Allocator object provides a useful set of tools to better understand the status of the allocator. If in `DEBUG` mode, the allocator and supporting classes will record additional debug tracking information to better track down memory leaks and issues. To enable DEBUG mode, either enable Java assertions with `-ea` or pass the following system property to the VM when starting `-Darrow.memory.debug.allocator=true`. The BufferAllocator also provides a `BufferAllocator.toVerboseString()` which can be used in DEBUG mode to get extensive stacktrace information and events associated with various Allocator behaviors.