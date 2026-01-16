# ALP Encoding Specification

**Types:** FLOAT, DOUBLE | **Reference:** [SIGMOD 2024](https://dl.acm.org/doi/10.1145/3626717)

---

## 1. Layout (Grouped Metadata-at-Start)

```
[Header(8B)] [AlpInfo₀|AlpInfo₁|...] [ForInfo₀|ForInfo₁|...] [Data₀|Data₁|...]
             |<--- AlpInfo Array -->|<--- ForInfo Array -->|<-- Data Array -->|
```

AlpInfo (ALP-specific) and ForInfo (FOR-specific) stored separately, then data.
Total metadata: 9B per vector (float), 13B per vector (double).

### Page Header (8 bytes)

| Offset | Field | Size | Value |
|--------|-------|------|-------|
| 0 | version | 1B | 1 |
| 1 | mode | 1B | 0 (ALP) |
| 2 | integer_encoding | 1B | 0 (FOR+bit-pack) |
| 3 | log_vector_size | 1B | 10 (meaning 2^10 = 1024) |
| 4 | num_elements | 4B | total element count (uint32) |

**Notes:**
- `log_vector_size` = log2(vector_size). Actual size = `2^log_vector_size`.
- `num_elements` is uint32 because Parquet page headers use i32 for num_values.

### AlpInfo (4 bytes, fixed)

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | exponent | 1B | uint8, 0..10/18 |
| 1 | factor | 1B | uint8, 0..e |
| 2 | num_exceptions | 2B | uint16 |

### ForInfo (type-dependent)

**Float (5 bytes):**

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | frame_of_reference | 4B | uint32 |
| 4 | bit_width | 1B | uint8, 0..32 |

**Double (9 bytes):**

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | frame_of_reference | 8B | uint64 |
| 8 | bit_width | 1B | uint8, 0..64 |

### Data Section

```
[PackedValues] [ExceptionPos] [ExceptionVals]
```

Note: `num_elements` per vector is derived from page header:
- Vectors 1..N-1: `num_elements = vector_size` (1024)
- Last vector: `num_elements = total % vector_size` (or vector_size if evenly divisible)

Note: `bit_packed_size` is computed: `ceil(num_elements * bit_width / 8)`

### Data Sections

| Section | Size |
|---------|------|
| PackedValues | `ceil(num_elements * bit_width / 8)` |
| ExceptionPos | `num_exceptions * 2` |
| ExceptionVals | `num_exceptions * sizeof(T)` |

---

## 2. Encoding

### Formula

```
encoded[i] = round(value[i] * 10^e * 10^-f)
```

Where:
- `e` = exponent (0..10 for float, 0..18 for double)
- `f` = factor (0..e)
- `round(n) = int(n + M) - M` where M = 2^22+2^23 (float) or 2^51+2^52 (double)

### Exception Detection

```
exception if: decode(encode(v)) != v
            | isnan(v) | isinf(v) | v == -0.0
            | v > MAX_INT | v < MIN_INT
```

### Frame of Reference (FOR)

```
FOR = min(encoded[])
delta[i] = encoded[i] - FOR
```

### Bit Packing

```
bit_width = ceil(log2(max(delta) + 1))
bit_packed_size = ceil(num_elements * bit_width / 8)
```

If `max(delta) == 0`: `bit_width = 0`, no packed data.

---

## 3. Decoding

```
delta[i]   = unpack(packed, bit_width)
encoded[i] = delta[i] + FOR
value[i]   = encoded[i] * 10^-f * 10^-e
value[exception_pos[j]] = exception_val[j]  // patch
```

---

## 4. Examples

### Example 1: No Exceptions

**Input:** `[1.23, 4.56, 7.89, 0.12]` (float)

| Step | Computation | Result |
|------|-------------|--------|
| e=2, f=0 | `v * 100` | `[123, 456, 789, 12]` |
| FOR | `min = 12` | `delta = [111, 444, 777, 0]` |
| bit_width | `ceil(log2(778))` | 10 |
| packed_size | `ceil(4*10/8)` | 5B |

**Output:** 9B (info, float) + 5B (packed) = **14B**

### Example 2: With Exceptions

**Input:** `[1.5, NaN, 2.5, 0.333...]` (float)

| Step | Result |
|------|--------|
| e=1, f=0 | `[15, -, 25, 3]` |
| Exceptions | pos=[1,3], vals=[NaN, 0.333...] |
| Placeholders | `[15, 15, 25, 15]` |
| FOR=15 | `delta = [0, 0, 10, 0]` |
| bit_width=4 | packed_size = 2B |

**Output:** 9B (info, float) + 2B (packed) + 4B (pos) + 8B (vals) = **23B**

### Example 3: 1024 Monetary Values ($0.01-$999.99)

| Metric | Value |
|--------|-------|
| e=2, f=0 | range: 1..99999 |
| bit_width | ceil(log2(99999)) = 17 |
| packed_size | ceil(1024*17/8) = 2176B |
| **Total (float)** | 9B + 2176B = ~2185B vs 4096B PLAIN (**47% smaller**) |

---

## 5. Constants

| Constant | Value |
|----------|-------|
| Vector size | 1024 |
| Version | 1 |
| Max combinations | 5 |
| Samples/vector | 256 |
| Float max_e | 10 |
| Double max_e | 18 |

---

## 6. Size Formulas

**Per vector:**
```
# H = VectorInfo header size (10 for float, 14 for double)
size = H + ceil(n * bw / 8) + exc * (2 + sizeof(T))
```

**Max compressed size:**
```
# H = VectorInfo header size (10 for float, 14 for double)
max = 8 + ceil(n/1024) * H + n * sizeof(T) * 2 + n * 2
```

---

## 7. Comparison

| Encoding | Compression | Best For |
|----------|-------------|----------|
| PLAIN | 1.0x | - |
| BYTE_STREAM_SPLIT | ~0.8x | random floats |
| ALP | ~0.5x | decimal floats |

---

## Appendix: Byte Layout

**Page Header (8 bytes):**
```
Offset  Field
------  -----
0       version
1       compression_mode
2       integer_encoding
3       log_vector_size (actual = 2^log_vector_size)
4-7     num_elements (uint32, total count)
```

**VectorInfo (Float, 9 bytes):**
```
Offset  Field
------  -----
0-3     frame_of_reference (uint32)
4       exponent
5       factor
6       bit_width
7       reserved
8-9     num_exceptions
10      packed_values[P] (P = ceil(n * bw / 8))
10+P    exception_pos[num_exceptions]
10+P+2E exception_vals[num_exceptions]
```

**VectorInfo (Double, 13 bytes):**
```
Offset  Field
------  -----
0-7     frame_of_reference (uint64)
8       exponent
9       factor
10      bit_width
11      reserved
12-13   num_exceptions
14      packed_values[P] (P = ceil(n * bw / 8))
14+P    exception_pos[num_exceptions]
14+P+2E exception_vals[num_exceptions]
```

Where `n = num_elements for this vector`, `P = bit_packed_size`, `E = num_exceptions`
