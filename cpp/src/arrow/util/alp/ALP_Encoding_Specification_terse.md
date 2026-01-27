# ALP Encoding Specification

**Types:** FLOAT, DOUBLE | **Reference:** [SIGMOD 2024](https://dl.acm.org/doi/10.1145/3626717)

---

## 1. Layout (Offset-Based Interleaved)

```
[Header(8B)] [Offset₀|Offset₁|...|Offₙ₋₁] [Vector₀][Vector₁]...[Vectorₙ₋₁]
             |<-- 4B per vector -------->|<-- Interleaved Vectors ------->|
```

Each vector = `[AlpInfo(4B)|ForInfo(5/9B)|Data]` stored contiguously.
Offsets enable O(1) random access to any vector.

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
- `num_vectors` = ceil(num_elements / vector_size)

### Offset Array (4B × num_vectors)

Each offset is a uint32 pointing to start of corresponding vector (from start of offsets section).

```
Offset₀ = num_vectors × 4           // First vector after all offsets
Offsetᵢ = Offsetᵢ₋₁ + sizeof(Vecᵢ₋₁)  // Subsequent vectors
```

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

### Data Section (per vector)

```
[PackedValues] [ExceptionPos] [ExceptionVals]
```

| Section | Size |
|---------|------|
| PackedValues | `ceil(num_elements * bit_width / 8)` |
| ExceptionPos | `num_exceptions * 2` |
| ExceptionVals | `num_exceptions * sizeof(T)` |

Note: `num_elements` per vector derived from page header:
- Vectors 0..N-2: `vector_size` (1024)
- Last vector: `total % vector_size` (or vector_size if evenly divisible)

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
// Read offset for vector K (O(1) random access)
offset_k = offsets[k]

// Jump to vector, read metadata inline
alp_info = read(offset_k)
for_info = read(offset_k + 4)

// Decode
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

**Output:** 8B(hdr) + 4B(off) + 4B(alp) + 5B(for) + 5B(pack) = **26B**

### Example 2: With Exceptions

**Input:** `[1.5, NaN, 2.5, 0.333...]` (float)

| Step | Result |
|------|--------|
| e=1, f=0 | `[15, -, 25, 3]` |
| Exceptions | pos=[1,3], vals=[NaN, 0.333...] |
| Placeholders | `[15, 15, 25, 15]` |
| FOR=15 | `delta = [0, 0, 10, 0]` |
| bit_width=4 | packed_size = 2B |

**Output:** 8B(hdr) + 4B(off) + 4B(alp) + 5B(for) + 2B(pack) + 4B(pos) + 8B(val) = **35B**

### Example 3: 3 Vectors (3072 float elements)

| Component | Size |
|-----------|------|
| Header | 8B |
| Offset Array | 3 × 4B = 12B |
| Per-Vector Metadata | 3 × 9B = 27B |
| Packed Values (bw=12) | 3 × ceil(1024×12/8) = 4608B |
| **Total** | ~4655B vs 12288B PLAIN (**62% smaller**) |

---

## 5. Constants

| Constant | Value |
|----------|-------|
| Default vector size | 1024 (configurable via log_vector_size) |
| Version | 1 |
| Max combinations | 5 |
| Samples/vector | 256 |
| Float max_e | 10 |
| Double max_e | 18 |
| OffsetType | uint32 (4 bytes) |

---

## 6. Size Formulas

**Offset array overhead:**
```
offset_overhead = num_vectors × 4 bytes
               ≈ 0.1% of input (4B per 1024 elements × sizeof(T))
```

**Per vector:**
```
size = AlpInfo(4B) + ForInfo(5/9B) + ceil(n × bw / 8) + exc × (2 + sizeof(T))
```

**Max compressed size:**
```
max = 8                              // header
    + num_vectors × 4                // offsets
    + num_vectors × (4 + ForInfo)    // metadata per vector
    + n × sizeof(T) × 2              // worst case: all packed + all exceptions
    + n × 2                          // exception positions
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

**Complete Page Example (3 float vectors, 3000 elements):**
```
Offset  Content
------  -------
0-7     Header (version=1, mode=0, log_vs=10, n=3000)
8-11    Offset₀ = 12 (first vector at byte 12 from offset array start)
12-15   Offset₁ (computed after Vector₀)
16-19   Offset₂ (computed after Vector₁)
20-23   Vector₀ AlpInfo (e, f, num_exc)
24-28   Vector₀ ForInfo (FOR, bw)
29-...  Vector₀ Data (packed, exc_pos, exc_val)
...     Vector₁ [AlpInfo|ForInfo|Data]
...     Vector₂ [AlpInfo|ForInfo|Data]
```

**Vector Layout (Float):**
```
Offset  Field
------  -----
0       exponent (uint8)
1       factor (uint8)
2-3     num_exceptions (uint16)
4-7     frame_of_reference (uint32)
8       bit_width (uint8)
9       packed_values[P] (P = ceil(n × bw / 8))
9+P     exception_pos[num_exceptions × 2B]
9+P+2E  exception_vals[num_exceptions × 4B]
```

**Vector Layout (Double):**
```
Offset  Field
------  -----
0       exponent (uint8)
1       factor (uint8)
2-3     num_exceptions (uint16)
4-11    frame_of_reference (uint64)
12      bit_width (uint8)
13      packed_values[P] (P = ceil(n × bw / 8))
13+P    exception_pos[num_exceptions × 2B]
13+P+2E exception_vals[num_exceptions × 8B]
```

Where `n = num_elements for this vector`, `P = bit_packed_size`, `E = num_exceptions`
