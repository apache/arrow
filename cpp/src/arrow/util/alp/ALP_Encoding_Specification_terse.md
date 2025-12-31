# ALP Encoding Specification

**Types:** FLOAT, DOUBLE | **Reference:** [SIGMOD 2024](https://dl.acm.org/doi/10.1145/3626717)

---

## 1. Layout

```
[Page Header (8B)] [Vector 1] [Vector 2] ... [Vector N]
```

### Page Header (8 bytes)

| Offset | Field | Size | Value |
|--------|-------|------|-------|
| 0 | version | 1B | 1 |
| 1 | mode | 1B | 0 (ALP) |
| 2 | layout | 1B | 0 (normal) |
| 3 | reserved | 1B | 0 |
| 4 | vector_size | 4B | 1024 |

### Vector

```
[VectorInfo (24B)] [PackedValues] [ExceptionPos] [ExceptionVals]
```

### VectorInfo (24 bytes)

| Offset | Field | Size | Type |
|--------|-------|------|------|
| 0 | frame_of_reference | 8B | uint64 |
| 8 | exponent | 1B | uint8, 0..18 |
| 9 | factor | 1B | uint8, 0..e |
| 10 | bit_width | 1B | uint8, 0..64 |
| 11 | reserved | 1B | - |
| 12 | num_elements | 2B | uint16, <=1024 |
| 14 | num_exceptions | 2B | uint16 |
| 16 | bit_packed_size | 2B | uint16 |
| 18 | padding | 6B | - |

### Data Sections

| Section | Size |
|---------|------|
| PackedValues | `bit_packed_size` |
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

**Output:** 24B (info) + 5B (packed) = **29B**

### Example 2: With Exceptions

**Input:** `[1.5, NaN, 2.5, 0.333...]` (float)

| Step | Result |
|------|--------|
| e=1, f=0 | `[15, -, 25, 3]` |
| Exceptions | pos=[1,3], vals=[NaN, 0.333...] |
| Placeholders | `[15, 15, 25, 15]` |
| FOR=15 | `delta = [0, 0, 10, 0]` |
| bit_width=4 | packed_size = 2B |

**Output:** 24B + 2B + 4B + 8B = **38B**

### Example 3: 1024 Monetary Values ($0.01-$999.99)

| Metric | Value |
|--------|-------|
| e=2, f=0 | range: 1..99999 |
| bit_width | ceil(log2(99999)) = 17 |
| packed_size | ceil(1024*17/8) = 2176B |
| **Total** | ~2200B vs 4096B PLAIN (**46% smaller**) |

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
size = 24 + ceil(n * bw / 8) + exc * (2 + sizeof(T))
```

**Max compressed size:**
```
max = 8 + ceil(n/1024) * 24 + n * sizeof(T) * 2 + n * 2
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

```
Offset  Field
------  -----
0-7     frame_of_reference
8       exponent
9       factor
10      bit_width
11      reserved
12-13   num_elements
14-15   num_exceptions
16-17   bit_packed_size
18-23   padding
24      packed_values[bit_packed_size]
24+P    exception_pos[num_exceptions]
24+P+2E exception_vals[num_exceptions]
```

Where `P = bit_packed_size`, `E = num_exceptions`
