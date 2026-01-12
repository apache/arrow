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

ALP encoding consists of a page-level header followed by one or more encoded vectors. Each vector contains up to 1024 elements.

### 2.1 Page Layout Diagram

```
+------------------------------------------------------------------+
|                           ALP PAGE                               |
+-------------+-------------+-------------+-------+----------------+
| Page Header |  Vector 1   |  Vector 2   |  ...  |   Vector N     |
| (16 bytes)  | (variable)  | (variable)  |       |  (variable)    |
+-------------+-------------+-------------+-------+----------------+
```

### 2.2 Page Header (16 bytes)

| Offset | Field            | Size    | Type   | Description                        |
|--------|------------------|---------|--------|------------------------------------|
| 0      | version          | 1 byte  | uint8  | Format version (must be 1)         |
| 1      | compression_mode | 1 byte  | uint8  | Compression mode (0 = ALP)         |
| 2      | bit_pack_layout  | 1 byte  | uint8  | Bit packing layout (0 = normal)    |
| 3      | reserved         | 1 byte  | uint8  | Reserved for future use            |
| 4      | vector_size      | 4 bytes | uint32 | Elements per vector (must be 1024) |
| 8      | num_elements     | 8 bytes | uint64 | Total element count in this page   |

```
Page Header Layout (16 bytes)
+---------+------------------+----------------+----------+------------------+------------------+
| version | compression_mode | bit_pack_layout| reserved |   vector_size    |   num_elements   |
| 1 byte  |     1 byte       |     1 byte     |  1 byte  |     4 bytes      |     8 bytes      |
|  0x01   |      0x00        |      0x00      |   0x00   |   0x00000400     |  (total count)   |
+---------+------------------+----------------+----------+------------------+------------------+
   Byte 0        Byte 1           Byte 2        Byte 3      Bytes 4-7          Bytes 8-15
```

### 2.3 Encoded Vector Structure

```
+----------------------------------------------------------------------------+
|                           ENCODED VECTOR                                   |
+-------------------+-----------------+--------------------+------------------+
|    VectorInfo     |  Packed Values  | Exception Positions| Exception Values |
|    (14 bytes)     |   (variable)    |    (variable)      |   (variable)     |
+-------------------+-----------------+--------------------+------------------+
```

### 2.4 VectorInfo Structure (14 bytes)

| Offset | Field              | Size     | Type   | Description                             |
|--------|--------------------|----------|--------|-----------------------------------------|
| 0      | frame_of_reference | 8 bytes  | uint64 | Minimum encoded value (FOR baseline)    |
| 8      | exponent           | 1 byte   | uint8  | Decimal exponent e (0-18 for double)    |
| 9      | factor             | 1 byte   | uint8  | Decimal factor f (0 <= f <= e)          |
| 10     | bit_width          | 1 byte   | uint8  | Bits per packed value (0-64)            |
| 11     | reserved           | 1 byte   | uint8  | Reserved (padding)                      |
| 12     | num_exceptions     | 2 bytes  | uint16 | Number of exception values              |

**Note:** The following fields are NOT stored in VectorInfo:
- `num_elements`: Derived from the page header. For vectors 1..N-1, it equals `vector_size` (1024). For the last vector, it equals `num_elements % vector_size` (or `vector_size` if evenly divisible).
- `bit_packed_size`: Computed on-demand as `ceil(num_elements * bit_width / 8)`.

### 2.5 Data Section Sizes

| Section             | Size Formula                              | Description                    |
|---------------------|-------------------------------------------|--------------------------------|
| Packed Values       | ceil(num_elements x bit_width / 8) bytes  | Bit-packed delta values        |
| Exception Positions | num_exceptions x 2 bytes                  | uint16 indices of exceptions   |
| Exception Values    | num_exceptions x sizeof(T)                | Original float/double values   |

---

## 3. Encoding Algorithm

### 3.1 Compression Pipeline

```
                    Input: float/double array
                              |
                              v
    +--------------------------------------------------------------+
    |  1. SAMPLING & PRESET GENERATION                             |
    |     * Sample vectors from dataset                            |
    |     * Try all (exponent, factor) combinations                |
    |     * Select best k combinations for preset                  |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  2. DECIMAL ENCODING                                         |
    |     encoded[i] = round(value[i] x 10^exponent x 10^-factor)  |
    |     Detect exceptions where decode(encode(v)) != v           |
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
                   Output: Serialized bytes
```

### 3.2 Sampling and Preset Generation

Before encoding, the algorithm samples data to determine optimal exponent/factor combinations:

| Parameter            | Value | Description                          |
|----------------------|-------|--------------------------------------|
| Vector Size          | 1024  | Elements compressed as a unit        |
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

### 3.3 Decimal Encoding Formula

```
+---------------------------------------------------------------------+
|                                                                     |
|   encoded[i] = round( value[i] x 10^exponent x 10^(-factor) )       |
|                                                                     |
|              = round( value[i] x 10^(exponent - factor) )           |
|                                                                     |
+---------------------------------------------------------------------+
```

Fast rounding uses a "magic number" technique:

| Type   | Magic Number                      | Formula                    |
|--------|-----------------------------------|----------------------------|
| float  | 2^22 + 2^23 = 12,582,912          | int((n + magic) - magic)   |
| double | 2^51 + 2^52 = 6,755,399,441,055,744 | int((n + magic) - magic) |

### 3.4 Exception Handling

A value becomes an exception if any of the following is true:

| Condition         | Example          | Reason                         |
|-------------------|------------------|--------------------------------|
| NaN               | float("nan")     | Cannot convert to integer      |
| Infinity          | float("inf")     | Cannot convert to integer      |
| Negative zero     | -0.0             | Would become +0.0 after encoding |
| Out of range      | +/-10^20 for double | Exceeds integer limits      |
| Round-trip failure| 3.333... with e=1, f=0 | decode(encode(v)) != v  |

Exception values are replaced with a placeholder (the first non-exception encoded value) to maintain the FOR encoding efficiency. The original values are stored separately.

### 3.5 Frame of Reference (FOR)

```
+--------------------------------------------------------------------------+
|  Encoded:  [ 123,  456,  789,   12 ]                                     |
|                                                                          |
|  min_value = 12  (stored as frame_of_reference)                          |
|                                                                          |
|  Deltas:   [ 111,  444,  777,    0 ]  <-- All non-negative!              |
+--------------------------------------------------------------------------+
```

### 3.6 Bit Packing

| Step                | Formula                              | Example                   |
|---------------------|--------------------------------------|---------------------------|
| 1. Find max delta   | max_delta = max(deltas)              | 777                       |
| 2. Calculate bit width | bit_width = ceil(log2(max_delta + 1)) | ceil(log2(778)) = 10   |
| 3. Pack values      | Each value uses bit_width bits       | 4 x 10 = 40 bits = 5 bytes|

Special case: If all values are identical, bit_width = 0 and no packed data is stored.

---

## 4. Decoding Algorithm

```
                    Input: Serialized bytes
                              |
                              v
    +--------------------------------------------------------------+
    |  1. BIT UNPACKING                                            |
    |     Extract bit_width from VectorInfo                        |
    |     Unpack num_elements values from packed data              |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  2. REVERSE FOR                                              |
    |     encoded[i] = delta[i] + frame_of_reference               |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  3. DECIMAL DECODING                                         |
    |     value[i] = encoded[i] x 10^(-factor) x 10^(-exponent)    |
    +--------------------------------------------------------------+
                              |
                              v
    +--------------------------------------------------------------+
    |  4. PATCH EXCEPTIONS                                         |
    |     value[pos[j]] = exceptions[j] for each exception         |
    +--------------------------------------------------------------+
                              |
                              v
                  Output: Original float/double array
```

---

## 5. Worked Examples

### 5.1 Example 1: Simple Decimal Values

**Input Data:**

```c
float values[4] = { 1.23, 4.56, 7.89, 0.12 };
```

**Step 1: Find Best Exponent/Factor**

Testing (exponent=2, factor=0) means multiply by 10^2 = 100:

| Value | value x 100 | Rounded | Verify: int x 0.01 | Match? |
|-------|-------------|---------|---------------------|--------|
| 1.23  | 123.0       | 123     | 1.23                | Yes    |
| 4.56  | 456.0       | 456     | 4.56                | Yes    |
| 7.89  | 789.0       | 789     | 7.89                | Yes    |
| 0.12  | 12.0        | 12      | 0.12                | Yes    |

All values round-trip correctly --> No exceptions!

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
packed_size = ceil(4 x 10 / 8) = 5 bytes
```

**Final Serialized Output:**

| Section            | Content                              | Size     |
|--------------------|--------------------------------------|----------|
| VectorInfo         | FOR=12, e=2, f=0, bw=10, exc=0       | 14 bytes |
| Packed Values      | 111, 444, 777, 0 (10 bits each)      | 5 bytes  |
| Exception Positions| (none)                               | 0 bytes  |
| Exception Values   | (none)                               | 0 bytes  |
| **TOTAL**          | -                                    | **19 bytes** |

Compression ratio: 16 bytes input --> 19 bytes output (overhead due to small input)

Note: With 1024 values, the 14-byte header overhead becomes negligible.

---

### 5.2 Example 2: With Exceptions

**Input Data:**

```c
float values[4] = { 1.5, NaN, 2.5, 0.333333... };
```

**Step 1: Decimal Encoding with (e=1, f=0)**

Multiply by 10^1 = 10:

| Index | Value     | value x 10 | Rounded | Verify         | Exception?          |
|-------|-----------|------------|---------|----------------|---------------------|
| 0     | 1.5       | 15.0       | 15      | 1.5 (ok)       | No                  |
| 1     | NaN       | -          | -       | -              | Yes (NaN)           |
| 2     | 2.5       | 25.0       | 25      | 2.5 (ok)       | No                  |
| 3     | 0.333...  | 3.333...   | 3       | 0.3 != 0.333...| Yes (round-trip fail) |

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
packed_size = ceil(4 x 4 / 8) = 2 bytes
```

**Final Serialized Output:**

| Section            | Content                              | Size     |
|--------------------|--------------------------------------|----------|
| VectorInfo         | FOR=15, e=1, f=0, bw=4, exc=2        | 14 bytes |
| Packed Values      | 0, 0, 10, 0 (4 bits each)            | 2 bytes  |
| Exception Positions| [1, 3]                               | 4 bytes  |
| Exception Values   | [NaN, 0.333...]                      | 8 bytes  |
| **TOTAL**          | -                                    | **28 bytes** |

---

### 5.3 Example 3: Monetary Data (1024 values)

**Input Data:**

1024 price values ranging from $0.01 to $999.99 (e.g., product prices)

```
Example values: 19.99, 5.49, 149.00, 0.99, 299.99, ...
```

**Optimal Encoding: (e=2, f=0)**

| Metric         | Value        | Calculation                         |
|----------------|--------------|-------------------------------------|
| Exponent       | 2            | Multiply by 100 for 2 decimal places|
| Factor         | 0            | No additional scaling needed        |
| Encoded range  | 1 to 99,999  | $0.01 --> 1, $999.99 --> 99999      |
| FOR min        | 1            | Assuming $0.01 is present           |
| Delta range    | 0 to 99,998  | After FOR subtraction               |
| Bit width      | 17           | ceil(log2(99999)) = 17 bits         |
| Packed size    | 2,176 bytes  | ceil(1024 x 17 / 8)                 |

**Size Comparison:**

| Encoding       | Size         | Ratio              |
|----------------|--------------|---------------------|
| PLAIN (float)  | 4,096 bytes  | 1.0x                |
| ALP            | ~2,200 bytes | 0.54x (46% smaller) |

---

## 6. Characteristics

| Property    | Description                                                                 |
|-------------|-----------------------------------------------------------------------------|
| Lossless    | All original floating-point values are perfectly recoverable, including NaN, Inf, -0.0 |
| Adaptive    | Exponent/factor selection adapts per vector based on data characteristics   |
| Vectorized  | Fixed 1024-element vectors enable SIMD-optimized bit packing/unpacking      |
| Exception-safe | Values that don't fit decimal model stored separately                    |

### 6.1 Best Use Cases

- Monetary/financial data (prices, transactions)
- Sensor readings with fixed precision
- Scientific measurements with limited decimal places
- GPS coordinates and geographic data
- Timestamps stored as floating-point

### 6.2 Worst Case Scenarios

- Random floating-point values (high exception rate)
- High-precision scientific data (many decimal places)
- Data with many special values (NaN, Inf)
- Very small datasets (header overhead dominates)

### 6.3 Comparison with Other Encodings

| Encoding            | Type Support   | Compression | Best For              |
|---------------------|----------------|-------------|-----------------------|
| PLAIN               | All            | None        | General purpose       |
| BYTE_STREAM_SPLIT   | Float/Double   | Moderate    | Random floats         |
| ALP                 | Float/Double   | High        | Decimal-like floats   |
| DELTA_BINARY_PACKED | Int32/Int64    | High        | Sequential integers   |

---

## 7. Constants Reference

| Constant                        | Value  | Description                         |
|---------------------------------|--------|-------------------------------------|
| kAlpVectorSize                  | 1024   | Elements per compressed vector      |
| kAlpVersion                     | 1      | Current format version              |
| kMaxCombinations                | 5      | Max (e,f) pairs in preset           |
| kSamplerSamplesPerVector        | 256    | Samples taken per vector            |
| kSamplerSampleVectorsPerRowgroup| 8      | Sample vectors per rowgroup         |
| Float max exponent              | 10     | 10^10 ~ 10 billion                  |
| Double max exponent             | 18     | 10^18 ~ 1 quintillion               |

---

## 8. Size Calculations

### 8.1 Vector Size Formula

```
vector_size = sizeof(VectorInfo)           // 14 bytes
            + bit_packed_size              // ceil(num_elements x bit_width / 8)
            + num_exceptions x 2           // exception positions (uint16)
            + num_exceptions x sizeof(T)   // exception values
```

### 8.2 Maximum Compressed Size

```
max_size = sizeof(PageHeader)              // 16 bytes
         + num_vectors x sizeof(VectorInfo)  // 14 bytes each
         + num_elements x sizeof(T)          // worst case: all values packed
         + num_elements x sizeof(T)          // worst case: all exceptions
         + num_elements x 2                  // exception positions

where num_vectors = ceil(num_elements / 1024)
```

### 8.3 Typical Compression Ratios

```
+------------------+-------------------+-------------------+
|    Data Type     |    Input Size     |   ALP Size        |
+------------------+-------------------+-------------------+
|                  |                   |                   |
|  Monetary data   |  4 bytes/value    |  ~2 bytes/value   |
|  (2 decimals)    |                   |  (50% reduction)  |
|                  |                   |                   |
+------------------+-------------------+-------------------+
|                  |                   |                   |
|  Sensor data     |  8 bytes/value    |  ~3 bytes/value   |
|  (3 decimals)    |                   |  (62% reduction)  |
|                  |                   |                   |
+------------------+-------------------+-------------------+
|                  |                   |                   |
|  Random floats   |  4 bytes/value    |  ~6 bytes/value   |
|  (many exceptions)|                  |  (expansion)      |
|                  |                   |                   |
+------------------+-------------------+-------------------+
```

---

## Appendix A: Byte Layout Diagram

### Page Header (16 bytes)

```
Byte Offset   Content
-----------   -------------------------------------------------------
0             version (uint8)
1             compression_mode (uint8)
2             bit_pack_layout (uint8)
3             reserved (uint8)
4-7           vector_size (uint32, little-endian)
8-15          num_elements (uint64, little-endian) - total element count
```

### Complete Vector Serialization (VectorInfo: 14 bytes)

```
Byte Offset   Content
-----------   -------------------------------------------------------
0-7           frame_of_reference (uint64, little-endian)
8             exponent (uint8)
9             factor (uint8)
10            bit_width (uint8)
11            reserved (uint8)
12-13         num_exceptions (uint16, little-endian)
14            +-----------------------------------------+
              |                                         |
              |        Packed Values                    |
              |        (P = ceil(n * bit_width / 8))    |
              |                                         |
14+P          +-----------------------------------------+
              |                                         |
              |        Exception Positions              |
              |        (num_exceptions x 2 bytes)       |
              |        [pos0, pos1, pos2, ...]          |
              |                                         |
14+P+E*2      +-----------------------------------------+
              |                                         |
              |        Exception Values                 |
              |        (num_exceptions x sizeof(T))     |
              |        [val0, val1, val2, ...]          |
              |                                         |
              +-----------------------------------------+

where n = num_elements for this vector (from page header)
      P = bit_packed_size = ceil(n * bit_width / 8)
      E = num_exceptions
```

---

## Appendix B: Algorithm Pseudocode

### Encoding

```
function EncodeALP(values[], num_values):
    // 1. Sampling phase
    preset = GeneratePreset(SampleValues(values))
    
    // 2. Process each vector
    for each vector of 1024 values:
        // Find best (e, f) for this vector
        (e, f) = FindBestExponentFactor(vector, preset)
        
        // Encode values
        for i = 0 to len(vector):
            encoded[i] = round(vector[i] * 10^e * 10^-f)
            decoded = encoded[i] * 10^-f * 10^-e
            if decoded != vector[i]:
                exceptions.add(vector[i])
                exception_positions.add(i)
                encoded[i] = placeholder
        
        // Frame of reference
        min_val = min(encoded)
        for i = 0 to len(encoded):
            delta[i] = encoded[i] - min_val
        
        // Bit packing
        bit_width = ceil(log2(max(delta) + 1))
        packed = BitPack(delta, bit_width)
        
        // Serialize
        output.write(VectorInfo)
        output.write(packed)
        output.write(exception_positions)
        output.write(exceptions)
```

### Decoding

```
function DecodeALP(bytes[], num_elements):
    // For each vector
    while bytes remaining and elements decoded < num_elements:
        // Read metadata
        info = ReadVectorInfo(bytes)
        
        // Bit unpack
        delta = BitUnpack(bytes, info.bit_width, info.num_elements)
        
        // Reverse FOR
        for i = 0 to info.num_elements:
            encoded[i] = delta[i] + info.frame_of_reference
        
        // Decode
        (e, f) = (info.exponent, info.factor)
        for i = 0 to info.num_elements:
            output[i] = encoded[i] * 10^-f * 10^-e
        
        // Patch exceptions
        positions = ReadExceptionPositions(bytes, info.num_exceptions)
        values = ReadExceptionValues(bytes, info.num_exceptions)
        for j = 0 to info.num_exceptions:
            output[positions[j]] = values[j]
    
    return output
```

---

*Document generated from Arrow ALP implementation*
*Reference: https://dl.acm.org/doi/10.1145/3626717*

