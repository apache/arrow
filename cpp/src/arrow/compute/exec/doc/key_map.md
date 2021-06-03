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

# Swiss Table

A specialized hash table implementation used to dynamically map combinations of key field values to a dense set of integer ids. Ids can later be used in place of keys to identify groups of rows with equal keys.

## Introduction

Hash group-by in Arrow uses a variant of a hash table based on a data structure called Swiss table. Swiss table uses linear probing. There is an array of slots and the information related to inserted keys is stored in these slots. A hash function determines the slot where the search for a matching key will start during hash table lookup. Then the slots are visited sequentially, wrapping around the end of an array, until either a match or an empty slot is found, the latter case meaning that there is no match.  Swiss table organizes the slots in blocks of 8 and has a design that enables data level parallelism at the block level. More precisely, it allows for visiting all slots within a block at once during lookups, by simply using 64-bit arithmetic. SIMD instructions can further enhance this data level parallelism allowing to process multiple blocks related to multiple input keys together using SIMD vectors of 64-bit elements. Occupied slots within a block are always clustered together. The name Swiss table comes from likening resulting sequences of empty slots to holes in a one dimensional cheese.

## Interface

Hash table used in query processing for implementing join and group-by operators does not need to provide all of the operations that a general purpose hash table would. Simplified requirements can help achieve a simpler and more efficient design. For instance we do not need to be able to remove previously inserted keys. It’s an append-only data structure: new keys can be added but old keys are never erased. Also, only a single copy of each key can be inserted - it is like `std::map` in that sense and not `std::multimap`.

Our Swiss table is fully vectorized. That means that all methods work on vectors of input keys processing them in batches. Specialized SIMD implementations of processing functions are almost always provided for performance critical operations. All callback interfaces used from the core hash table code are also designed to work on batches of inputs instead of individual keys. The batch size can be almost arbitrary and is selected by the client of the hash table. Batch size should be the smallest number of input items, big enough so that the benefits of vectorization and SIMD can be fully experienced. Keeping it small means less memory used for temporary arrays storing intermediate results of computation (vector equivalent of some temporary variables kept on the stack). That in turn means smaller space in CPU caches, which also means less impact on other memory access intensive operations. We pick 1024 as the default size of the batch. We will call it a **mini-batch** to distinguish it from potentially other forms of batches used at higher levels in the code, e.g. when scheduling work for worker threads or relational operators inside an analytic query.

The main functionality provided by Swiss table is mapping of arbitrarily complex keys to unique integer ids. Let us call it **lookup-or-insert**. Given a sequence of key values, return a corresponding sequence of integer ids, such that all keys that are equal receive the same id and for K distinct keys the integer ids will be assigned from the set of numbers 0 to (K-1). If we find a matching key in a hash table for a given input, we return the **key id** assigned when the key was first inserted into a hash table. If we fail to find an already inserted match, we assign the first unused integer as a key id and add a new entry to a hash table. Due to vectorized processing, which may result in out-of-order processing of individual inputs, it is not guaranteed that if there are two new key values in the same input batch and one of them appears earlier in the input sequence, then it will receive a smaller key id. Additional mapping functionality can be built on top of basic mapping to integer key id, for instance if we want to assign and perhaps keep updating some values to all unique keys, we can keep these values in a resizable vector indexed by obtained key id.

The implementation of Swiss table does not need to have any information related to the domain of the keys. It does not use their logical data type or information about their physical representation and does not even use pointers to keys. All access to keys is delegated to a separate class or classes that provide callback functions for three operations:
-   computing hashes of keys;
-   checking equality for given pairs of keys;
-   appending a given sequence of keys to a stack maintained outside of Swiss table object, so that they can be referenced later on by key ids (key ids will be equal to their positions in the stack).


When passing arguments to callback functions the keys are referenced using integer ids. For the left side - that is the keys present in the input mini-batch - ordinal positions within that mini-batch are used. For the right side - that is the keys inserted into the hash table - these are identified by key ids assigned to them and stored inside Swiss table when they were first encountered and processed.

Diagram with logical view of information passing in callbacks:

![alt text](img/key_map_1.jpg)

Hash table values for inserted keys are also stored inside Swiss table. Because of that, hash table logic does not need to ever re-evaluate the hash, and there is actually no need for a hash function callback. It is enough that the caller provides hash values for all entries in the batch when calling lookup-or-insert.

## Basic architecture and organization of data
The hash table is an array of **slots**. Slots are grouped in groups of 8 called **blocks**. The number of blocks is a power of 2. The empty hash table starts with a single block, with all slots empty. Then, as the keys are getting inserted and the amount of empty slots is shrinking, at some point resizing of the hash table is triggered. The data stored in slots is moved to a new hash table that has the double of the number of blocks.

The diagram below shows the basic organization of data in our implementation of Swiss table:

![alt text](img/key_map_2.jpg)

N is the log of the number of blocks, 2<sup>n+3</sup> is  the number of slots and also the maximum number of inserted keys and hence (N + 3) is the number of bits required to store a key id. We will refer to N as the **size of the hash table**.

Index of a block within an array will be called **block id**, and similarly index of a slot will be **slot id**. Sometimes we will focus on a single block and refer to slots that belong to it by using a **local slot id**, which is an index from 0 to 7.

Every slot can either be **empty** or store data related to a single inserted key. There are three pieces of information stored inside a slot:
- status byte,
- key id,
- key hash.

Status byte, as the name suggests, stores 8 bits. The highest bit indicates if the slot is empty (the highest bit is set) or corresponds to one of inserted keys (the highest bit is zero). The remaining 7 bits contain 7 bits of key hash that we call a **stamp**. The stamp is used to eliminate some false positives when searching for a matching key for a given input. Slot also stores **key id**, which is a non-negative integer smaller than the number of inserted keys, that is used as a reference to the actual inserted key. The last piece of information related to an inserted key is its **hash** value. We store hashes for all keys, so that they never need to be re-computed. That greatly simplifies some operations, like resizing of a hash table, that may not even need to look at the keys at all. For an empty slot, the status byte is 0x80, key id is zero and the hash is not used and can be set to any number.

A single block contains 8 slots and can be viewed as a micro-stack of up to 8 inserted keys. When the first key is inserted into an empty block, it will occupy a slot with local id 0. The second inserted key will go into slot number 1 and so on. We use N highest bits of hash to get an index of a **start block**, when searching for a match or an empty slot to insert a previously not seen key when that is the case. If the start block contains any empty slots, then the search for either a match or place to insert a key will end at that block. We will call such a block an **open block**. A block that is not open is a full block. In the case of full block, the input key related search may continue in the next block module the number of blocks. If the key is not inserted into its start block, we will refer to it as an **overflow** entry, other entries being **non-overflow**. Overflow entries are slower to process, since they require visiting more than one block, so we want to keep their percentage low. This is done by choosing the right **load factor** (percentage of occupied slots in the hash table) at which the hash table gets resized and the number of blocks gets doubled. By tuning this value we can control the probability of encountering an overflow entry.

The most interesting part of each block is the set of status bytes of its slots, which is simply a single 64-bit word. The implementation of efficient searches across these bytes during lookups require using either leading zero count or trailing zero count intrinsic. Since there are cases when only the first one is available, in order to take advantage of it, we order the bytes in the 64-bit status word so that the first slot within a block uses the highest byte and the last one uses the lowest byte (slots are in reversed bytes order). The diagram below shows how the information about slots is stored within a 64-bit status word:

![alt text](img/key_map_3.jpg)

Each status byte has a 7-bit fragment of hash value - a **stamp** - and an empty slot bit. Empty slots have status byte equal to 0x80 - the highest bit is set to 1 to indicate an empty slot and the lowest bits, which are used by a stamp, are set to zero.

The diagram below shows which bits of hash value are used by hash table:

![alt text](img/key_map_4.jpg)

If a hash table has 2<sup>N</sup> blocks, then we use N highest bits of a hash to select a start block when searching for a match. The next 7 bits are used as a stamp. Using the highest bits to pick a start block means that a range of hash values can be easily mapped to a range of block ids of start blocks for hashes in that range. This is useful when resizing a hash table or merging two hash tables together.

### Interleaving status bytes and key ids

Status bytes and key ids for all slots are stored in a single array of bytes. They are first grouped by 8 into blocks, then each block of status bytes is interleaved with a corresponding block of key ids. Finally key ids are represented using the smallest possible number of bits and bit-packed (bits representing each next key id start right after the last bit of the previous key id). Note that regardless of the chosen number of bits, a block of bit-packed key ids (that is 8 of them) will start and end on the byte boundary.

The diagram below shows the organization of bytes and bits of a single block in interleaved array:
![alt text](img/key_map_5.jpg)

From the size of the hash table we can derive the number K of bits needed in the worst case to encode any key id. K is equal to the number of bits needed to represent slot id (number of keys is not greater than the number of slots and any key id is strictly less than the number of keys), which for a hash table of size N (N blocks) equals (N+3). To simplify bit packing and unpacking and avoid handling of special cases, we will round up K to full bytes for K > 24 bits.

Status bytes are stored in a single 64-bit word in reverse byte order (the last byte corresponds to the slot with local id 0). On the other hand key ids are stored in the normal order (the order of slot ids).

Since both status byte and key id for a given slot are stored in the same array close to each other, we can expect that most of the lookups will read only one CPU cache-line from memory inside Swiss table code (then at least another one outside Swiss table to access the bytes of the key for the purpose of comparison). Even if we hit an overflow entry, it is still likely to reside on the same cache-line as the start block data. Hash values, which are stored separately from status byte and key id, are only used when resizing and do not impact the lookups outside these events.

> Improvement to consider:
> In addition to the Swiss table data, we need to store an array of inserted keys, one for each key id. If keys are of fixed length, then the address of the bytes of the key can be calculated by multiplying key id by the common length of the key. If keys are of varying length, then there will be an additional array with an offset of each key within the array of concatenated bytes of keys. That means that any key comparison during lookup will involve 3 arrays: one to get key id, one to get key offset and final one with bytes of the key. This could be reduced to 2 array lookups if we stored key offset instead of key id interleaved with slot status bytes. Offset indexed by key id and stored in its own array becomes offset indexed by slot id and stored interleaved with slot status bytes. At the same time key id indexed by slot id and interleaved with slot status bytes before becomes key id referenced using offset and stored with key bytes. There may be a slight increase in the total size of memory needed by the hash table, equal to the difference in the number of bits used to store offset and those used to store key id, multiplied by the number of slots, but that should be a small fraction of the total size.

### 32-bit hash vs 64-bit hash

Currently we use 32-bit hash values in Swiss table code and 32-bit integers as key ids. For the robust implementation, sooner or later we will need to support 64-bit hash and 64-bit key ids. When we use 32-bit hash, it means that we run out of hash bits when hash table size N is greater than 25 (25 bits of hash needed to select a block and 7 bits needed to generate a stamp byte reach 32 total bits). When the number of inserted keys exceeds the maximal number of keys stored in a hash table of size 25 (which is at least 2<sup>24</sup>), the chance of false positives during lookups will start quickly growing. 32-bit hash should not be used with more than about 16 million inserted keys.

### Low memory footprint and low chance of hash collisions

Swiss table is a good choice of a hash table for modern hardware, because it combines lookups that can take advantage of special CPU instructions with space efficiency and low chance of hash collisions.

Space efficiency is important for performance, because the cost of random array accesses, often dominating the lookup cost for larger hash tables, increases with the size of the arrays. This happens due to limited space of CPU caches. Let us look at what is the amortized additional storage cost for a key in a hash table apart from the essential cost of storing data of all those keys. Furthermore, we can skip the storage of hash values, since these are only used during infrequent hash table resize operations (should not have a big impact on CPU cache usage in normal cases).

Half full hash table of size N will use 2 status bytes per inserted key (because for every filled slot there is one empty slot) and 2\*(N+3) bits for key id (again, one for the occupied slot and one for the empty). For N = 16 for instance this is slightly under 7 bytes per inserted key.

Swiss table also has a low probability of false positives leading to wasted key comparisons. Here is some rationale behind why this should be the case. Hash table of size N can contain up to 2<sup>N+3</sup> keys. Search for a match involves (N + 7) hash bits: N to select a start block and 7 to use as a stamp. There are always at least 16 times more combinations of used hash bits than there are keys in the hash table (32 times more if the hash table is half full). These numbers mean that the probability of false positives resulting from a search for a matching slot should be low. That corresponds to an expected number of comparisons per lookup being close to 1 for keys already present and 0 for new keys.

## Lookup

Lookup-or-insert operation, given a hash of a key, finds a list of candidate slots with corresponding keys that are likely to be equal to the input key. The list may be empty, which means that the key does not exist yet in the hash table. If it is not empty, then the callback function for key comparison is called for each next candidate to verify that there is indeed a match. False positives get rejected and we end up either finding an actual match or an empty slot, which means that the key is new to the hash table. New keys get assigned next available integers as key ids, and are appended to the set of keys stored in the hash table. As a result of inserting new keys to the hash table, the density of occupied slots may reach an upper limit, at which point the hash table will be resized and will afterwards have twice as many slots. That is in summary lookup-or-insert functionality, but the actual implementation is a bit more involved, because of vectorization of the processing and various optimizations for common cases.

### Search within a single block

There are three possible cases that can occur when searching for a match for a given key (that is, for a given stamp of a key) within a single block, illustrated below.

 1. There is a matching stamp in the block of status bytes:

![alt text](img/key_map_6.jpg)

 2. There is no matching stamp in the block, but there is an empty slot in the block: 

![alt text](img/key_map_7.jpg)

 3. There is no matching stamp in the block and the block is full (there are no empty slots left): 

![alt text](img/key_map_8.jpg)

64-bit arithmetic can be used to search for a matching slot within the entire single block at once, without iterating over all slots in it. Following is an example of a sequence of steps to find the first status byte for a given stamp, returning the first empty slot on miss if the block is not full or 8 (one past maximum local slot id) otherwise.

Following is a sketch of the possible steps to execute when searching for the matching stamp in a single block. 

*Example will use input stamp 0x5E and a 64-bit status bytes word with one empty slot:  
0x 4B17 5E3A 5E2B 1180*.

1. [1 instruction] Replicate stamp to all bytes by multiplying it by 0x 0101 0101 0101 0101.  

	*We obtain: 0x 5E5E 5E5E 5E5E 5E5E.*

2. [1 instruction] XOR replicated stamp with status bytes word. Bytes corresponding to a matching stamp will be 0, bytes corresponding to empty slots will have a value between 128 and 255, bytes corresponding to non-matching non-empty slots will have a value between 1 and 127.

	*We obtain: 0x 1549 0064 0075 4FDE.*

3. [2 instructions] In the next step we want to have information about a match in the highest bit of each byte. We can ignore here empty slot bytes, because they will be taken care of at a later step. Set the highest bit in each byte (OR with 0x 8080 8080 8080 8080) and then subtract 1 from each byte (subtract 0x 0101 0101 0101 0101 from 64-bit word). Now if a byte corresponds to a non-empty slot then the highest bit 0 indicates a match and 1 indicates a miss.

	*We obtain: 0x 95C9 80E4 80F5 CFDE, 
	then 0x 94C8 7FE3 7FF4 CEDD.*

4. [3 instructions] In the next step we want to obtain in each byte one of two values: 0x80 if it is either an empty slot or a match, 0x00 otherwise. We do it in three steps: NOT the result of the previous step to change the meaning of the highest bit; OR with the original status word to set highest bit in a byte to 1 for empty slots; mask out everything other than the highest bits in all bytes (AND with 0x 8080 8080 8080 8080).

	*We obtain: 6B37 801C 800B 3122,  
	then 6B37 DE3E DE2B 31A2,  
	finally 0x0000 8000 8000 0080.*

5. [2 instructions] Finally, use leading zero bits count and divide it by 8 to find an index of the last byte that corresponds either to a match or an empty slot. If the leading zero count intrinsic returns 64 for a 64-bit input zero, then after dividing by 8 we will also get the desired answer in case of a full block without any matches.

	*We obtain: 16,  
	then 2 (index of the first slot within the block that matches the stamp).*

If SIMD instructions with 64-bit lanes are available, multiple single block searches for different keys can be executed together. For instance AVX2 instruction set allows to process quadruplets of 64-bit values in a single instruction, four searches at once.

### Complete search potentially across multiple blocks

Full implementation of a search for a matching key may involve visiting multiple blocks beginning with the start block selected based on the hash of the key. We move to the next block modulo the number of blocks, whenever we do not find a match in the current block and the current block is full. The search may also involve visiting one or more slots in each block. Visiting in this case means calling a comparison callback to verify the match whenever a slot with a matching stamp is encountered. Eventually the search stops when either:  
- the matching key is found in one of the slots matching the stamp, or

- an empty slot is reached. This is illustrated in the diagram below:
![alt text](img/key_map_9.jpg)


### Optimistic processing with two passes

Hash table lookups may have high cost in the pessimistic case, when we encounter cases of hash collisions and full blocks that lead to visiting further blocks. In the majority of cases we can expect an optimistic situation - the start block is not full, so we will only visit this one block, and all stamps in the block are different, so we will need at most one comparison to find a match. We can expect about 90% of the key lookups for an existing key to go through the optimistic path of processing. For that reason it pays off to optimize especially for this 90% of inputs.

Lookups in Swiss table are split into two passes over an input batch of keys. The **first pass:  fast-path lookup** , is a highly optimized, vectorized, SIMD-friendly, branch-free code that fully handles optimistic cases. The **second pass: slow-path lookup** , is normally executed only for the selection of inputs that have not been finished in the first pass, although it can also be called directly on all of the inputs, skipping fast-path lookup. It handles all special cases and inserts but in order to be robust it is not as efficient as fast-path. Slow-path lookup does not need to repeat the work done in fast-path lookup - it can use the state reached at the end of fast-path lookup as a starting point.

Fast-path lookup implements search only for the first stamp match and only within the start block. It only makes sense when we already have at least one key inserted into the hash table, since it does not handle inserts. It takes a vector of key hashes as an input and based on it outputs three pieces of information for each key:

- Key id corresponding to the slot in which a matching stamp was found. Any valid key id if a matching stamp was not found.
- A flag indicating if a match was found or not.  
- Slot id of a slot from which slow-path should pick up the search if the first match was either not found or it turns out to be false positive after evaluating key comparison.

> Improvement to consider: 
> precomputing 1st pass lookup results.
> 
> If the hash table is small, the number of inserted keys is small, we could further simplify and speed-up the first pass by storing in a lookup table pre-computed results for all combinations of hash bits. Let us consider the case of Swiss table of size 5 that has 256 slots and up to 128 inserted keys. Only 12 bits of hash are used by lookup in that case: 5 to select a block, 7 to create a stamp. For all 2<sup>12</sup> combinations of those bits we could keep the result of first pass lookup in an array. Key id and a match indicating flag can use one byte: 7 bits for key id and 1 bit for the flag. Note that slot id is only needed if we go into 2nd pass lookup, so it can be stored separately and likely only accessed by a small subset of keys. Fast-path lookup becomes almost a single fetch of result from a 4KB array. Lookup arrays used to implement this need to be kept in sync with the main copy of data about slots, which requires extra care during inserts. Since the number of entries in lookup arrays is much higher than the number of slots, this technique only makes sense for small hash tables.

### Dense comparisons

If there is at least one key inserted into a hash table, then every slot contains a key id value that corresponds to some actual key that can be used in comparison. That is because empty slots are initialized with 0 as their key id. After the fast-path lookup we get a match-found flag for each input. If it is set, then we need to run a comparison of the input key with the key in the hash table identified by key id returned by fast-path code. The comparison will verify that there is a true match between the keys. We only need to do this for a subset of inputs that have a match candidate, but since we have key id values corresponding to some real key for all inputs, we may as well execute comparisons on all inputs unconditionally. If the majority (e.g. more than 80%) of the keys have a match candidate, the cost of evaluating comparison for the remaining fraction of keys but without filtering may actually be cheaper than the cost of running evaluation only for required keys while referencing filter information. This can be seen as a variant of general preconditioning techniques used to avoid diverging conditional branches in the code. It may be used, based on some heuristic, to verify matches reported by fast-path lookups and is referred to as **dense comparisons**.

## Resizing

New hash table is initialized as empty and has only a single block with a space for only a few key entries. Doubling of the hash table size becomes necessary as more keys get inserted. It is invoked during the 2nd pass of the lookups, which also handles inserts. It happens immediately after the number of inserted keys reaches a specific upper limit decided based on a current size of the hash table. There may still be unprocessed entries from the input mini-batch after resizing, so the 2nd pass of the lookup is restarted right after, with the bigger hash table and the remaining subset of unprocessed entries.

Current policy, that should work reasonably well, is to resize a small hash table (up to 8KB) when it is 50% full. Larger hash tables are resized when 75% full. We want to keep size in memory as small as possible, while maintaining a low probability of blocks becoming full.
  
When discussing resizing we will be talking about **resize source** and **resize target** tables. The diagram below shows how the same hash bits are interpreted differently by the source and the target.

![alt text](img/key_map_10.jpg)

For a given hash, if a start block id was L in the source table, it will be either (2\*L+0) or (2\*L+1) in the target table. Based on that we can expect data access locality when migrating the data between the tables.

Resizing is cheap also thanks to the fact that hash values for keys in the hash table are kept together with other slot data and do not need to be recomputed. That means that resizing procedure does not ever need to access the actual bytes of the key.

### 1st pass

Based on the hash value for a given slot we can tell whether this slot contains an overflow or non-overflow entry. In the first pass we go over all source slots in sequence, filter out overflow entries and move to the target table all other entries. Non-overflow entries from a block L will be distributed between blocks (2\*L+0) and (2\*L+1) of the target table. None of these target blocks can overflow, since they will be accommodating at most 8 input entries during this pass.

For every non-overflow entry, the highest bit of a stamp in the source slot decides whether it will go to the left or to the right target block. It is further possible to avoid any conditional branches in this partitioning code, so that the result is friendly to the CPU execution pipeline.

![alt text](img/key_map_11.jpg)


### 2nd pass

In the second pass of resizing, we scan all source slots again, this time focusing only on the overflow entries that were all skipped in the 1st pass. We simply reinsert them in the target table using generic insertion code with one exception. Since we know that all the source keys are different, there is no need to search for a matching stamp or run key comparisons (or look at the key values). We just need to find the first open block beginning with the start block in the target table and use its first empty slot as the insert destination.

We expect overflow entries to be rare and therefore the relative cost of that pass should stay low.

