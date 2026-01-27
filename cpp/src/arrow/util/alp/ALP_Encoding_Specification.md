# ALP Encoding Specification

*Adaptive Lossless floating-Point Compression*

---

## 1. Overview

### 1.1 Supported Types

| Data Type | Integer Type | Max Exponent | Value Range |
|-----------|--------------|--------------|-------------|
| FLOAT     | INT32        | 10           | +/-2,147,483,520 |
| DOUBLE    | INT64        | 18           | +/-9.22 x 10^18 |

This encoding is adapted from the Adaptive Lossless floating-Point (ALP) compression algorithm described in "ALP: Adaptive Lossless floating-Point Compression" (SIGMOD 2024, https://dl.acm.org/doi/10.1145/3626717).

ALP works by converting floating-point values to integers using decimal scaling, then applying frame of reference (FOR) encoding and bit-packing. Values that cannot be losslessly converted are stored as exceptions. The encoding achieves high compression for decimal-like floating-point data (e.g., monetary values, sensor readings) while remaining fully lossless.

---

## 2. Data Layout

ALP encoding consists of a page-level header followed by an offset array and one or more encoded vectors. The vector size is configurable (default: 1024 elements, specified via `log_vector_size` in the header).

### 2.1 Page Layout Diagram (Offset-Based Interleaved)

The page uses an **offset-based interleaved** layout for O(1) random access.

```
+-----------------------------------------------------------------------------------+
|                                    ALP PAGE                                        |
+--------+-----------------------------------+--------------------------------------+
| Header |         Offset Array              |        Interleaved Vectors           |
| (8B)   | [Off₀|Off₁|Off₂|...|Offₙ₋₁]       | [Vec₀][Vec₁][Vec₂]...[Vecₙ₋₁]        |
+--------+-----------------------------------+--------------------------------------+
         |<---- 4 bytes per vector --------->|
         |                                   |
         |   Byte offset from start of       |
         |   compression body (after header) |
```

Each offset is a **uint32** (4 bytes) that points to the start of the corresponding vector's data within the compression body. The offset is measured from the start of the compression body (i.e., after the 8-byte page header).

### 2.2 Interleaved Vector Structure

Each vector is stored contiguously with its metadata immediately preceding its data:

```
+-------------------+-------------------+------------------------------------------+
|     AlpInfo       |     ForInfo       |                  Data                    |
|    (4 bytes)      | (5B float/9B dbl) | [PackedValues][ExcPositions][ExcValues]  |
+-------------------+-------------------+------------------------------------------+
```

This interleaved layout provides:

| Benefit | Description |
|---------|-------------|
| **O(1) Random Access** | Jump directly to any vector using its offset—no cumulative computation needed |
| **Better Locality** | Metadata + data together for each vector reduces cache misses |
| **Parallel Decompression** | Each vector is self-contained; threads can decode independently |
| **Storage Overhead** | 4 bytes per vector (~0.4% for typical 100KB pages) |

### 2.3 Page Header (8 bytes, fixed)

| Offset | Field            | Size    | Type   | Description                        |
|--------|------------------|---------|--------|------------------------------------|
| 0      | version          | 1 byte  | uint8  | Format version (must be 1)         |
| 1      | compression_mode | 1 byte  | uint8  | Compression mode (0 = ALP)         |
| 2      | integer_encoding | 1 byte  | uint8  | Integer encoding method (0 = FOR+bit-pack) |
| 3      | log_vector_size  | 1 byte  | uint8  | Log2 of vector size (10 = 1024)    |
| 4      | num_elements     | 4 bytes | uint32 | Total element count in this page   |

**Notes:**
- `log_vector_size` stores the base-2 logarithm of the vector size. The actual vector size is computed as `2^log_vector_size`. For example, 10 means 2^10 = 1024 elements per vector.
- `num_elements` is uint32 because Parquet page headers use i32 for num_values. See: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
- Number of vectors is computed as: `num_vectors = ceil(num_elements / vector_size)`

```
Page Header Layout (8 bytes)
+---------+------------------+------------------+------------------+------------------+
| version | compression_mode | integer_encoding | log_vector_size  |   num_elements   |
| 1 byte  |     1 byte       |     1 byte       |     1 byte       |     4 bytes      |
|  0x01   |      0x00        |      0x00        |      0x0A        |  (little-endian) |
+---------+------------------+------------------+------------------+------------------+
   Byte 0        Byte 1           Byte 2             Byte 3            Bytes 4-7
```

### 2.4 Offset Array (4 bytes × num_vectors)

Immediately after the header, an array of byte offsets enables O(1) random access to any vector:

| Offset (from header end) | Field  | Size    | Type   | Description |
|--------------------------|--------|---------|--------|-------------|
| 0                        | Off₀   | 4 bytes | uint32 | Byte offset to Vector₀ (from start of compression body) |
| 4                        | Off₁   | 4 bytes | uint32 | Byte offset to Vector₁ |
| 8                        | Off₂   | 4 bytes | uint32 | Byte offset to Vector₂ |
| ...                      | ...    | ...     | ...    | ... |
| 4×(n-1)                  | Offₙ₋₁ | 4 bytes | uint32 | Byte offset to Vectorₙ₋₁ |

**Offset Calculation:**

```
Offset₀ = num_vectors × 4                    // First vector starts after all offsets
Offsetᵢ = Offsetᵢ₋₁ + sizeof(Vectorᵢ₋₁)     // Each subsequent vector follows the previous
```

**Example with 3 float vectors:**

```
Compression Body Layout:
+-------+-------+-------+-------------------+-------------------+-------------------+
| Off₀  | Off₁  | Off₂  |     Vector₀       |     Vector₁       |     Vector₂       |
| (4B)  | (4B)  | (4B)  | AlpI|ForI|Data    | AlpI|ForI|Data    | AlpI|ForI|Data    |
+-------+-------+-------+-------------------+-------------------+-------------------+
^                       ^                   ^                   ^
|                       |                   |                   |
Offset 0                Off₀=12             Off₁                Off₂
```

### 2.5 AlpInfo Structure (fixed 4 bytes)

AlpInfo contains ALP-specific metadata that is independent of the integer encoding:

| Offset | Field          | Size     | Type   | Description                             |
|--------|----------------|----------|--------|-----------------------------------------|
| 0      | exponent       | 1 byte   | uint8  | Decimal exponent e (0-10 for float, 0-18 for double) |
| 1      | factor         | 1 byte   | uint8  | Decimal factor f (0 ≤ f ≤ e)            |
| 2      | num_exceptions | 2 bytes  | uint16 | Number of exception values (little-endian) |

```
AlpInfo Layout (4 bytes)
+----------+--------+------------------+
| exponent | factor |  num_exceptions  |
|  1 byte  | 1 byte |     2 bytes      |
+----------+--------+------------------+
   Byte 0    Byte 1      Bytes 2-3
```

### 2.6 ForInfo Structure (type-dependent size)

ForInfo contains FOR (Frame of Reference) encoding metadata. The size depends on type:
- **Float:** 5 bytes (4-byte frame_of_reference + 1-byte bit_width)
- **Double:** 9 bytes (8-byte frame_of_reference + 1-byte bit_width)

#### Float ForInfo (5 bytes)

| Offset | Field              | Size     | Type   | Description                             |
|--------|--------------------|----------|--------|-----------------------------------------|
| 0      | frame_of_reference | 4 bytes  | uint32 | Minimum encoded value (FOR baseline)    |
| 4      | bit_width          | 1 byte   | uint8  | Bits per packed value (0-32)            |

```
Float ForInfo Layout (5 bytes)
+------------------------------+-----------+
|     frame_of_reference       | bit_width |
|          4 bytes             |   1 byte  |
+------------------------------+-----------+
        Bytes 0-3                 Byte 4
```

#### Double ForInfo (9 bytes)

| Offset | Field              | Size     | Type   | Description                             |
|--------|--------------------|----------|--------|-----------------------------------------|
| 0      | frame_of_reference | 8 bytes  | uint64 | Minimum encoded value (FOR baseline)    |
| 8      | bit_width          | 1 byte   | uint8  | Bits per packed value (0-64)            |

```
Double ForInfo Layout (9 bytes)
+----------------------------------------------+-----------+
|            frame_of_reference                | bit_width |
|                   8 bytes                    |   1 byte  |
+----------------------------------------------+-----------+
                   Bytes 0-7                      Byte 8
```

**Total per-vector metadata size:**
- **Float:** AlpInfo (4) + ForInfo (5) = **9 bytes**
- **Double:** AlpInfo (4) + ForInfo (9) = **13 bytes**

### 2.7 Data Section Structure

Each vector's data section contains three parts in order:

| Section             | Size Formula                              | Description                    |
|---------------------|-------------------------------------------|--------------------------------|
| Packed Values       | ceil(num_elements × bit_width / 8) bytes  | Bit-packed delta values        |
| Exception Positions | num_exceptions × 2 bytes                  | uint16 indices of exceptions   |
| Exception Values    | num_exceptions × sizeof(T)                | Original float/double values   |

```
Data Section Layout
+------------------------------------------+------------------------+----------------------+
|            Packed Values                 |   Exception Positions  |   Exception Values   |
|   ceil(n × bit_width / 8) bytes          |   num_exc × 2 bytes    |  num_exc × sizeof(T) |
+------------------------------------------+------------------------+----------------------+
```

**Notes:**
- `num_elements` for a vector is NOT stored; it is derived from the page header:
  - Vectors 0 to N-2: `vector_size` elements (1024)
  - Last vector: `num_elements % vector_size` (or `vector_size` if evenly divisible)
- `bit_packed_size` is computed as: `ceil(num_elements × bit_width / 8)`
- If `bit_width = 0`, no packed values are stored (all values are identical after FOR)

---

## 3. Complete Page Layout Examples

### 3.1 Example: Float Page with 3 Vectors (3000 elements)

```
                           ALP PAGE (Float, 3000 elements)
+--------+-------+-------+-------+---------------------+---------------------+------------------+
| Header | Off₀  | Off₁  | Off₂  |      Vector₀        |      Vector₁        |    Vector₂       |
|  (8B)  | (4B)  | (4B)  | (4B)  |    1024 elements    |    1024 elements    |   952 elements   |
+--------+-------+-------+-------+---------------------+---------------------+------------------+
         |<-- Offset Array -->|  |<-------------- Interleaved Vectors ------------------->|
         |     12 bytes       |

Vector₀ Detail (1024 float elements, 5 exceptions, bit_width=12):
+----------+----------+-----------------+---------------------+-------------------+
| AlpInfo  | ForInfo  |  Packed Values  | Exception Positions | Exception Values  |
|   (4B)   |   (5B)   |    1536 bytes   |       10 bytes      |     20 bytes      |
+----------+----------+-----------------+---------------------+-------------------+
|<-- 9B -->|          |<-- ceil(1024×12/8) = 1536 bytes --->|<-- 5×2 -->|<-- 5×4 -->|

Total Vector₀ size: 9 + 1536 + 10 + 20 = 1575 bytes
```

### 3.2 Example: Double Page with 2 Vectors (1500 elements)

```
                           ALP PAGE (Double, 1500 elements)
+--------+-------+-------+----------------------+----------------------+
| Header | Off₀  | Off₁  |       Vector₀        |       Vector₁        |
|  (8B)  | (4B)  | (4B)  |    1024 elements     |     476 elements     |
+--------+-------+-------+----------------------+----------------------+
         |<- 8B ->|      |<-------- Interleaved Vectors ------------>|

Vector₁ Detail (476 double elements, 0 exceptions, bit_width=20):
+----------+----------+-----------------+
| AlpInfo  | ForInfo  |  Packed Values  |
|   (4B)   |   (9B)   |    1190 bytes   |
+----------+----------+-----------------+
|<-- 13B -->|         |<-- ceil(476×20/8) = 1190 bytes -->|

Total Vector₁ size: 13 + 1190 = 1203 bytes
```

---

## 4. Encoding Algorithm

### 4.1 Compression Pipeline

```
                    Input: float/double array
                              |
                              v
    +--------------------------------------------------------------+
    |  1. SAMPLING & PRESET GENERATION                             |
    |     • Sample vectors from dataset                            |
    |     • Try all (exponent, factor) combinations                |
    |     • Select best k combinations for preset                  |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  2. DECIMAL ENCODING                                         |
    |     encoded[i] = round(value[i] × 10^exponent × 10^-factor)  |
    |     Detect exceptions where decode(encode(v)) ≠ v            |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  3. FRAME OF REFERENCE (FOR)                                 |
    |     min_value = min(encoded[])                               |
    |     delta[i] = encoded[i] - min_value                        |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  4. BIT PACKING                                              |
    |     bit_width = ceil(log2(max_delta + 1))                    |
    |     Pack each delta into bit_width bits                      |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  5. SERIALIZE (Offset-Based Layout)                          |
    |     • Write page header                                      |
    |     • Reserve space for offset array                         |
    |     • For each vector: write AlpInfo|ForInfo|Data            |
    |     • Backfill offset array with computed offsets            |
    +--------------------------------------------------------------+
                              |
                              v
                   Output: Serialized bytes
```

### 4.2 Sampling and Preset Generation

Before encoding, the algorithm samples data to determine optimal exponent/factor combinations:

| Parameter            | Value | Description                          |
|----------------------|-------|--------------------------------------|
| Vector Size          | 1024  | Default elements per vector (configurable) |
| Sample Size          | 256   | Values sampled per vector            |
| Max Combinations     | 5     | Best (e,f) pairs kept in preset      |
| Early Exit Threshold | 4     | Stop if 4 consecutive worse results  |

Valid exponent/factor combinations:

```
For each exponent e from 0 to max_exponent:
    For each factor f from 0 to e:
        Try combination (e, f)
        
Float:  max_exponent = 10  -->  66 combinations
Double: max_exponent = 18  --> 190 combinations
```

### 4.3 Decimal Encoding Formula

```
+---------------------------------------------------------------------+
|                                                                     |
|   encoded[i] = round( value[i] × 10^exponent × 10^(-factor) )       |
|                                                                     |
|              = round( value[i] × 10^(exponent - factor) )           |
|                                                                     |
+---------------------------------------------------------------------+
```

Fast rounding uses a "magic number" technique:

| Type   | Magic Number                      | Formula                    |
|--------|-----------------------------------|----------------------------|
| float  | 2^22 + 2^23 = 12,582,912          | int((n + magic) - magic)   |
| double | 2^51 + 2^52 = 6,755,399,441,055,744 | int((n + magic) - magic) |

### 4.4 Exception Handling

A value becomes an exception if any of the following is true:

| Condition         | Example          | Reason                         |
|-------------------|------------------|--------------------------------|
| NaN               | float("nan")     | Cannot convert to integer      |
| Infinity          | float("inf")     | Cannot convert to integer      |
| Negative zero     | -0.0             | Would become +0.0 after encoding |
| Out of range      | +/-10^20 for double | Exceeds integer limits      |
| Round-trip failure| 3.333... with e=1, f=0 | decode(encode(v)) ≠ v  |

Exception values are replaced with a placeholder (the first non-exception encoded value) to maintain the FOR encoding efficiency. The original values are stored separately.

### 4.5 Frame of Reference (FOR)

```
+--------------------------------------------------------------------------+
|  Encoded:  [ 123,  456,  789,   12 ]                                     |
|                                                                          |
|  min_value = 12  (stored as frame_of_reference)                          |
|                                                                          |
|  Deltas:   [ 111,  444,  777,    0 ]  <-- All non-negative!              |
+--------------------------------------------------------------------------+
```

### 4.6 Bit Packing

| Step                | Formula                              | Example                   |
|---------------------|--------------------------------------|---------------------------|
| 1. Find max delta   | max_delta = max(deltas)              | 777                       |
| 2. Calculate bit width | bit_width = ceil(log2(max_delta + 1)) | ceil(log2(778)) = 10   |
| 3. Pack values      | Each value uses bit_width bits       | 4 × 10 = 40 bits = 5 bytes|

Special case: If all values are identical, bit_width = 0 and no packed data is stored.

---

## 5. Decoding Algorithm

### 5.1 Decompression Pipeline

```
                    Input: Serialized bytes
                              |
                              v
    +--------------------------------------------------------------+
    |  1. READ HEADER & OFFSET ARRAY                               |
    |     Parse page header to get num_elements, vector_size       |
    |     Compute num_vectors = ceil(num_elements / vector_size)   |
    |     Read offset array: num_vectors × 4 bytes                 |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  2. FOR EACH VECTOR (can be parallelized):                   |
    |     a. Jump to vector using offset                           |
    |     b. Read AlpInfo (4 bytes)                                |
    |     c. Read ForInfo (5/9 bytes)                              |
    |     d. Read packed values, positions, exceptions             |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  3. BIT UNPACKING                                            |
    |     Unpack num_elements values from packed data              |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  4. REVERSE FOR                                              |
    |     encoded[i] = delta[i] + frame_of_reference               |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  5. DECIMAL DECODING                                         |
    |     value[i] = encoded[i] × 10^(-factor) × 10^(-exponent)    |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  6. PATCH EXCEPTIONS                                         |
    |     value[pos[j]] = exceptions[j] for each exception         |
    +--------------------------------------------------------------+
                              |
                              v
                  Output: Original float/double array
```

### 5.2 Random Access to Vector K

The offset-based layout enables efficient random access:

```
function ReadVectorK(bytes[], k):
    // 1. Read header
    header = ReadHeader(bytes[0:8])
    num_vectors = ceil(header.num_elements / header.vector_size)
    
    // 2. Read offset for vector K directly (O(1))
    offset_k = ReadUInt32(bytes[8 + k*4 : 8 + k*4 + 4])
    
    // 3. Jump directly to vector K
    vector_start = 8 + offset_k    // 8 = header size
    
    // 4. Decode vector K
    return DecodeVector(bytes[vector_start:], header, k)
```

---

## 6. Worked Examples

### 6.1 Example 1: Simple Decimal Values

**Input Data:**

```c
float values[4] = { 1.23, 4.56, 7.89, 0.12 };
```

**Step 1: Find Best Exponent/Factor**

Testing (exponent=2, factor=0) means multiply by 10^2 = 100:

| Value | value × 100 | Rounded | Verify: int × 0.01 | Match? |
|-------|-------------|---------|---------------------|--------|
| 1.23  | 123.0       | 123     | 1.23                | Yes    |
| 4.56  | 456.0       | 456     | 4.56                | Yes    |
| 7.89  | 789.0       | 789     | 7.89                | Yes    |
| 0.12  | 12.0        | 12      | 0.12                | Yes    |

All values round-trip correctly → No exceptions!

**Step 2: Frame of Reference**

| Encoded | min = 12 | Delta (encoded - min) |
|---------|----------|------------------------|
| 123     | -        | 111                    |
| 456     | -        | 444                    |
| 789     | -        | 777                    |
| 12      | -        | 0                      |

**Step 3: Bit Packing**

```
max_delta = 777
bit_width = ceil(log2(778)) = 10 bits
packed_size = ceil(4 × 10 / 8) = 5 bytes
```

**Step 4: Serialization (Offset-Based Layout)**

Since we have 1 vector (4 elements < 1024), offset array has 1 entry:

| Section            | Content                              | Size     |
|--------------------|--------------------------------------|----------|
| Header             | version=1, mode=0, int_enc=0, log_vs=10, n=4 | 8 bytes |
| Offset Array       | [Off₀ = 4]                           | 4 bytes  |
| AlpInfo            | e=2, f=0, num_exc=0                  | 4 bytes  |
| ForInfo            | FOR=12, bw=10                        | 5 bytes  |
| Packed Values      | 111, 444, 777, 0 (10 bits each)      | 5 bytes  |
| Exception Positions| (none)                               | 0 bytes  |
| Exception Values   | (none)                               | 0 bytes  |
| **TOTAL**          |                                      | **26 bytes** |

---

### 6.2 Example 2: With Exceptions

**Input Data:**

```c
float values[4] = { 1.5, NaN, 2.5, 0.333333... };
```

**Step 1: Decimal Encoding with (e=1, f=0)**

Multiply by 10^1 = 10:

| Index | Value     | value × 10 | Rounded | Verify         | Exception?          |
|-------|-----------|------------|---------|----------------|---------------------|
| 0     | 1.5       | 15.0       | 15      | 1.5 (ok)       | No                  |
| 1     | NaN       | -          | -       | -              | Yes (NaN)           |
| 2     | 2.5       | 25.0       | 25      | 2.5 (ok)       | No                  |
| 3     | 0.333...  | 3.333...   | 3       | 0.3 ≠ 0.333...| Yes (round-trip fail) |

**Step 2: Handle Exceptions**

```
Exception positions: [1, 3]
Exception values: [NaN, 0.333333...]
Placeholder value: 15 (first non-exception encoded value)
Encoded array with placeholders: [15, 15, 25, 15]
```

**Step 3: Frame of Reference**

| Encoded            | min = 15 | Delta |
|--------------------|----------|-------|
| 15                 | -        | 0     |
| 15 (placeholder)   | -        | 0     |
| 25                 | -        | 10    |
| 15 (placeholder)   | -        | 0     |

**Step 4: Bit Packing**

```
max_delta = 10
bit_width = ceil(log2(11)) = 4 bits
packed_size = ceil(4 × 4 / 8) = 2 bytes
```

**Step 5: Serialization**

| Section            | Content                              | Size     |
|--------------------|--------------------------------------|----------|
| Header             | version=1, mode=0, int_enc=0, log_vs=10, n=4 | 8 bytes |
| Offset Array       | [Off₀ = 4]                           | 4 bytes  |
| AlpInfo            | e=1, f=0, num_exc=2                  | 4 bytes  |
| ForInfo            | FOR=15, bw=4                         | 5 bytes  |
| Packed Values      | 0, 0, 10, 0 (4 bits each)            | 2 bytes  |
| Exception Positions| [1, 3]                               | 4 bytes  |
| Exception Values   | [NaN, 0.333...]                      | 8 bytes  |
| **TOTAL**          |                                      | **35 bytes** |

---

### 6.3 Example 3: Multi-Vector Page (3072 float elements)

**Input Data:**

3072 price values: 3 full vectors of 1024 elements each.

**Serialization Layout:**

```
+--------+-------+-------+-------+-----------------+-----------------+-----------------+
| Header | Off₀  | Off₁  | Off₂  |    Vector₀      |    Vector₁      |    Vector₂      |
|  (8B)  | (4B)  | (4B)  | (4B)  |                 |                 |                 |
+--------+-------+-------+-------+-----------------+-----------------+-----------------+

Offsets (measured from start of compression body, after 8-byte header):
- Off₀ = 12           // 3 offsets × 4 bytes = 12 bytes
- Off₁ = 12 + V₀_size // Right after Vector₀
- Off₂ = Off₁ + V₁_size
```

**Size Breakdown (assuming bit_width=12, 5 exceptions per vector):**

| Component          | Formula                                      | Size       |
|--------------------|----------------------------------------------|------------|
| Header             | Fixed                                        | 8 bytes    |
| Offset Array       | 3 vectors × 4 bytes                          | 12 bytes   |
| Per-Vector Metadata| 3 × (4 + 5) = 3 × 9 bytes                    | 27 bytes   |
| Packed Values      | 3 × ceil(1024 × 12 / 8)                      | 4608 bytes |
| Exception Positions| 3 × 5 × 2 bytes                              | 30 bytes   |
| Exception Values   | 3 × 5 × 4 bytes                              | 60 bytes   |
| **TOTAL**          |                                              | **4745 bytes** |

**Compression Ratio:**
- Input: 3072 × 4 bytes = 12,288 bytes
- Output: 4,745 bytes
- Ratio: 38.6% (61.4% reduction)

---

## 7. Characteristics

| Property    | Description                                                                 |
|-------------|-----------------------------------------------------------------------------|
| Lossless    | All original floating-point values are perfectly recoverable, including NaN, Inf, -0.0 |
| Adaptive    | Exponent/factor selection adapts per vector based on data characteristics   |
| Vectorized  | Fixed 1024-element vectors enable SIMD-optimized bit packing/unpacking      |
| Exception-safe | Values that don't fit decimal model stored separately                    |
| Random Access | O(1) access to any vector via offset lookup                              |

### 7.1 Best Use Cases

- Monetary/financial data (prices, transactions)
- Sensor readings with fixed precision
- Scientific measurements with limited decimal places
- GPS coordinates and geographic data
- Timestamps stored as floating-point

### 7.2 Worst Case Scenarios

- Random floating-point values (high exception rate)
- High-precision scientific data (many decimal places)
- Data with many special values (NaN, Inf)
- Very small datasets (header overhead dominates)

### 7.3 Comparison with Other Encodings

| Encoding            | Type Support   | Compression | Best For              |
|---------------------|----------------|-------------|-----------------------|
| PLAIN               | All            | None        | General purpose       |
| BYTE_STREAM_SPLIT   | Float/Double   | Moderate    | Random floats         |
| ALP                 | Float/Double   | High        | Decimal-like floats   |
| DELTA_BINARY_PACKED | Int32/Int64    | High        | Sequential integers   |

---

## 8. Constants Reference

| Constant                        | Value  | Description                         |
|---------------------------------|--------|-------------------------------------|
| kAlpVectorSize                  | 1024   | Default elements per vector (configurable via log_vector_size) |
| kAlpVersion                     | 1      | Current format version              |
| kMaxCombinations                | 5      | Max (e,f) pairs in preset           |
| kSamplerSamplesPerVector        | 256    | Samples taken per vector            |
| kSamplerSampleVectorsPerRowgroup| 8      | Sample vectors per rowgroup         |
| Float max exponent              | 10     | 10^10 ~ 10 billion                  |
| Double max exponent             | 18     | 10^18 ~ 1 quintillion               |
| OffsetType                      | uint32 | 4 bytes per vector offset           |

---

## 9. Size Calculations

### 9.1 Vector Size Formula

```
vector_size = AlpInfo_size                     // 4 bytes (fixed)
            + ForInfo_size                     // 5 bytes (float) or 9 bytes (double)
            + bit_packed_size                  // ceil(num_elements × bit_width / 8)
            + num_exceptions × 2               // exception positions (uint16)
            + num_exceptions × sizeof(T)       // exception values
```

### 9.2 Page Size Formula

```
page_size = sizeof(Header)                     // 8 bytes
          + num_vectors × sizeof(OffsetType)   // 4 bytes per vector (offset array)
          + sum(vector_sizes)                  // all vectors including metadata
```

### 9.3 Maximum Compressed Size

```
max_size = sizeof(PageHeader)                  // 8 bytes
         + num_vectors × 4                     // offset array
         + num_vectors × (AlpInfo + ForInfo)   // 9 or 13 bytes each
         + num_elements × sizeof(T)            // worst case: all values packed at full width
         + num_elements × sizeof(T)            // worst case: all exceptions
         + num_elements × 2                    // exception positions

where num_vectors = ceil(num_elements / vector_size)
```

### 9.4 Offset Array Overhead Analysis

| Page Elements | Vectors | Offset Array Size | Overhead % |
|---------------|---------|-------------------|------------|
| 1,024         | 1       | 4 bytes           | 0.10%      |
| 10,240        | 10      | 40 bytes          | 0.10%      |
| 102,400       | 100     | 400 bytes         | 0.10%      |
| 1,024,000     | 1,000   | 4,000 bytes       | 0.10%      |

The offset array overhead is approximately **0.1% of input size** (4 bytes per vector_size elements × sizeof(T), assuming vector_size=1024).

### 9.5 Typical Compression Ratios

```
+------------------+-------------------+-------------------+-------------------+
|    Data Type     |    Input Size     |   ALP Size        | Compression Ratio |
+------------------+-------------------+-------------------+-------------------+
|                  |                   |                   |                   |
|  Monetary data   |  4 bytes/value    |  ~2 bytes/value   | 50% reduction     |
|  (2 decimals)    |                   |                   |                   |
|                  |                   |                   |                   |
+------------------+-------------------+-------------------+-------------------+
|                  |                   |                   |                   |
|  Sensor data     |  8 bytes/value    |  ~3 bytes/value   | 62% reduction     |
|  (3 decimals)    |                   |                   |                   |
|                  |                   |                   |                   |
+------------------+-------------------+-------------------+-------------------+
|                  |                   |                   |                   |
|  Random floats   |  4 bytes/value    |  ~6 bytes/value   | 50% expansion     |
|  (many exceptions)|                  |                   |                   |
|                  |                   |                   |                   |
+------------------+-------------------+-------------------+-------------------+
```

---

## Appendix A: Complete Byte Layout Diagram

### A.1 Page Header (8 bytes)

```
Byte Offset   Content
-----------   -------------------------------------------------------
0             version (uint8) = 0x01
1             compression_mode (uint8) = 0x00 (ALP)
2             integer_encoding (uint8) = 0x00 (FOR+BitPack)
3             log_vector_size (uint8) = 0x0A (10, meaning 1024)
4-7           num_elements (uint32, little-endian)
```

### A.2 Offset Array (4 × num_vectors bytes)

```
Byte Offset   Content
-----------   -------------------------------------------------------
8             offset_0 (uint32, little-endian) - offset to Vector₀
12            offset_1 (uint32, little-endian) - offset to Vector₁
...           ...
8+4*(n-1)     offset_{n-1} (uint32, little-endian) - offset to Vectorₙ₋₁

Note: Offsets are measured from byte 8 (start of compression body)
```

### A.3 Complete Vector Serialization (Float)

```
Byte Offset   Content
(relative     
to vector)   
-----------   -------------------------------------------------------
              --- AlpInfo (4 bytes) ---
0             exponent (uint8)
1             factor (uint8)
2-3           num_exceptions (uint16, little-endian)
              --- ForInfo (5 bytes) ---
4-7           frame_of_reference (uint32, little-endian)
8             bit_width (uint8)
              --- Data Section ---
9             +-----------------------------------------+
              |        Packed Values                    |
              |        P = ceil(n × bit_width / 8)      |
9+P           +-----------------------------------------+
              |        Exception Positions              |
              |        num_exceptions × 2 bytes         |
9+P+E×2       +-----------------------------------------+
              |        Exception Values                 |
              |        num_exceptions × 4 bytes         |
              +-----------------------------------------+

where n = num_elements for this vector
      P = bit_packed_size
      E = num_exceptions
```

### A.4 Complete Vector Serialization (Double)

```
Byte Offset   Content
(relative     
to vector)   
-----------   -------------------------------------------------------
              --- AlpInfo (4 bytes) ---
0             exponent (uint8)
1             factor (uint8)
2-3           num_exceptions (uint16, little-endian)
              --- ForInfo (9 bytes) ---
4-11          frame_of_reference (uint64, little-endian)
12            bit_width (uint8)
              --- Data Section ---
13            +-----------------------------------------+
              |        Packed Values                    |
              |        P = ceil(n × bit_width / 8)      |
13+P          +-----------------------------------------+
              |        Exception Positions              |
              |        num_exceptions × 2 bytes         |
13+P+E×2      +-----------------------------------------+
              |        Exception Values                 |
              |        num_exceptions × 8 bytes         |
              +-----------------------------------------+
```

### A.5 Complete Page Example (3 Float Vectors)

```
Byte      Content                           Notes
------    --------------------------------  ---------------------------
0-7       Header (8 bytes)                  version=1, n=3000
8-11      Offset₀ = 12                      First vector at byte 12
12-15     Offset₁ = 1587                    Second vector at byte 1587
16-19     Offset₂ = 3162                    Third vector at byte 3162
20-23     Vector₀ AlpInfo                   e=2, f=0, exc=5
24-28     Vector₀ ForInfo                   FOR=100, bw=12
29-1564   Vector₀ Packed (1536B)            1024 × 12 bits
1565-1574 Vector₀ Exc Pos (10B)             5 × 2 bytes
1575-1594 Vector₀ Exc Val (20B)             5 × 4 bytes
1595-...  Vector₁ (similar structure)       1024 elements
...       Vector₂ (similar structure)       952 elements (remainder)
```

---

## Appendix B: Algorithm Pseudocode

### B.1 Encoding

```
function EncodeALP(values[], num_values):
    // 1. Sampling phase
    preset = GeneratePreset(SampleValues(values))
    
    // 2. Calculate vector count
    vector_size = 1 << log_vector_size  // e.g., 1024 for log_vector_size=10
    num_vectors = ceil(num_values / vector_size)
    
    // 3. Write header
    output.write(Header{version=1, mode=0, int_enc=0, 
                        log_vs=10, num_elements=num_values})
    
    // 4. Reserve space for offset array
    offset_array_start = output.position()
    output.skip(num_vectors * 4)
    
    // 5. Track offsets
    offsets = []
    data_section_start = output.position()
    
    // 6. Encode each vector
    for v = 0 to num_vectors - 1:
        offsets.append(output.position() - data_section_start)
        
        vector = values[v*vector_size : min((v+1)*vector_size, num_values)]
        
        // Find best (e, f)
        (e, f) = FindBestExponentFactor(vector, preset)
        
        // Encode values
        encoded = []
        exceptions = []
        exception_positions = []
        
        for i = 0 to len(vector):
            enc = round(vector[i] * 10^e * 10^-f)
            dec = enc * 10^-f * 10^-e
            if dec != vector[i] or isSpecial(vector[i]):
                exceptions.append(vector[i])
                exception_positions.append(i)
                enc = placeholder
            encoded.append(enc)
        
        // Frame of reference
        min_val = min(encoded)
        deltas = [enc - min_val for enc in encoded]
        
        // Bit packing
        bit_width = ceil(log2(max(deltas) + 1))
        packed = BitPack(deltas, bit_width)
        
        // Write vector
        output.write(AlpInfo{e, f, len(exceptions)})
        output.write(ForInfo{min_val, bit_width})
        output.write(packed)
        output.write(exception_positions)
        output.write(exceptions)
    
    // 7. Backfill offset array
    end_position = output.position()
    output.seek(offset_array_start)
    for offset in offsets:
        output.write_uint32(offset)
    output.seek(end_position)
```

### B.2 Decoding

```
function DecodeALP(bytes[], num_elements):
    // 1. Read header
    header = ReadHeader(bytes[0:8])
    vector_size = 1 << header.log_vector_size
    num_vectors = ceil(header.num_elements / vector_size)
    
    // 2. Read offset array
    offsets = []
    for i = 0 to num_vectors - 1:
        offsets.append(ReadUInt32(bytes[8 + i*4]))
    
    data_section_start = 8  // After header
    output = []
    
    // 3. Decode each vector (can be parallelized!)
    for v = 0 to num_vectors - 1:
        // Jump to vector using offset
        vector_start = data_section_start + offsets[v]
        
        // Read metadata
        alp_info = ReadAlpInfo(bytes[vector_start:])
        for_info = ReadForInfo(bytes[vector_start + 4:])
        data_start = vector_start + 4 + sizeof(ForInfo)
        
        // Calculate vector element count
        if v < num_vectors - 1:
            vec_elements = vector_size
        else:
            vec_elements = header.num_elements - v * vector_size
        
        // Bit unpack
        packed_size = ceil(vec_elements * for_info.bit_width / 8)
        deltas = BitUnpack(bytes[data_start:], 
                          for_info.bit_width, vec_elements)
        
        // Reverse FOR
        encoded = [d + for_info.frame_of_reference for d in deltas]
        
        // Decode
        decoded = [enc * 10^(-alp_info.factor) * 10^(-alp_info.exponent) 
                   for enc in encoded]
        
        // Patch exceptions
        exc_pos_start = data_start + packed_size
        exc_val_start = exc_pos_start + alp_info.num_exceptions * 2
        
        positions = ReadUInt16Array(bytes[exc_pos_start:], alp_info.num_exceptions)
        values = ReadFloatArray(bytes[exc_val_start:], alp_info.num_exceptions)
        
        for j = 0 to alp_info.num_exceptions - 1:
            decoded[positions[j]] = values[j]
        
        output.extend(decoded)
    
    return output
```

### B.3 Random Access to Single Vector

```
function DecodeVectorK(bytes[], k):
    // 1. Read header
    header = ReadHeader(bytes[0:8])
    
    // 2. Read single offset (O(1))
    offset_k = ReadUInt32(bytes[8 + k*4])
    
    // 3. Jump directly to vector K
    vector_start = 8 + offset_k
    
    // 4. Decode single vector
    return DecodeSingleVector(bytes[vector_start:], header, k)
```

---

## Appendix C: Implementation Notes

### C.1 Preset-Based Encoding API

For performance-critical applications, the encoding can be split into two phases:

```cpp
// Phase 1: Pre-compute preset (can be done once, outside hot path)
auto preset = AlpWrapper<double>::CreateSamplingPreset(data, data_size);

// Phase 2: Encode using preset (fast, no sampling overhead)
AlpWrapper<double>::EncodeWithPreset(data, data_size, 
                                     comp_buf, &comp_size, preset);
```

This is particularly useful for:
- Benchmarking (removes sampling overhead from measurements)
- Batch processing (compute preset once, encode many batches)
- Streaming scenarios (sample initial data, use preset for subsequent chunks)

### C.2 Endianness

All multi-byte integers are stored in **little-endian** format:
- `num_elements` (uint32)
- `num_exceptions` (uint16)
- `frame_of_reference` (uint32/uint64)
- `offsets[]` (uint32)
- `exception_positions[]` (uint16)

### C.3 Alignment

The format does NOT require any specific alignment. All fields are packed without padding. Decoders should use byte-by-byte or unaligned access primitives.

---

*Document generated from Arrow ALP implementation*
*Reference: https://dl.acm.org/doi/10.1145/3626717*
