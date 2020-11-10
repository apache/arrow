#[cfg(all(test, target_endian = "big"))]
mod tests_bit_slices_big_endian {
    use super::*;
    use crate::datatypes::ToByteSlice;

    #[test]
    fn test_bit_slice_iter_aligned() {
        let input: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7];
        let buffer: Buffer = Buffer::from(input);

        let bit_slice = buffer.bit_slice();
        let result = bit_slice.chunks().interpret().collect::<Vec<u64>>();

        assert_eq!(vec![0x0001020304050607], result);
    }

    #[test]
    fn test_bit_slice_iter_unaligned() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000,
            0b00100000, 0b01000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bit_slice = buffer.bit_slice().view(4, 64);
        let chunks = bit_slice.chunks::<u64>();

        assert_eq!(0, chunks.remainder_bit_len());
        assert_eq!(0, chunks.remainder_bits());

        let result = chunks.interpret().collect::<Vec<u64>>();

        assert_eq!(
            vec![0b0001_00000010_00000100_00001000_00010000_00100000_01000000_1111],
            result
        );
    }

    #[test]
    fn test_bit_slice_iter_unaligned_remainder_1_byte() {
        let input: &[u8] = &[
            0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000,
            0b00100000, 0b01000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bit_slice = buffer.bit_slice().view(4, 66);
        let chunks = bit_slice.chunks::<u64>();

        assert_eq!(2, chunks.remainder_bit_len());
        assert_eq!(0b00000011, chunks.remainder_bits());

        let result = chunks.interpret().collect::<Vec<u64>>();

        assert_eq!(
            vec![0b0001_00000010_00000100_00001000_00010000_00100000_01000000_1111],
            result
        );
    }

    #[test]
    fn test_bit_slice_iter_unaligned_remainder_bits_across_bytes() {
        let input: &[u8] = &[0b00111111, 0b11111100];
        let buffer: Buffer = Buffer::from(input);

        // remainder contains bits from both bytes
        // result should be the highest 2 bits from first byte followed by lowest 5 bits of second bytes
        let bit_slice = buffer.bit_slice().view(6, 7);
        let chunks = bit_slice.chunks::<u64>();

        assert_eq!(7, chunks.remainder_bit_len());
        assert_eq!(0b01111111, chunks.remainder_bits());
    }

    #[test]
    fn test_bit_slice_iter_unaligned_remainder_bits_large() {
        let input: &[u8] = &[
            0b11111111, 0b00000000, 0b11111111, 0b00000000, 0b11111111, 0b00000000,
            0b11111111, 0b00000000, 0b11111111,
        ];
        let buffer: Buffer = Buffer::from(input);

        let bit_slice = buffer.bit_slice().view(2, 63);
        let chunks = bit_slice.chunks::<u64>();

        assert_eq!(63, chunks.remainder_bit_len());
        assert_eq!(
            0b1111110_00000001_11111110_00000001_11111110_00000001_11111110_00000001,
            chunks.remainder_bits()
        );
    }

    #[test]
    fn test_bit_slice_iter_reinterpret() {
        assert_eq!(LocalBits::default(), Msb0::default());
        let buffer_slice = &[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice();
        // Name of the bit slice comes from byte slice, since it is still on the stack and behaves similarly to Rust's byte slice.
        let buffer = Buffer::from(buffer_slice);

        // Let's get the whole buffer.
        let bit_slice = buffer.bit_slice().view(0, buffer_slice.len() * 8);
        // Let's also get a chunked bits as u8, not u64 this time...
        let chunks = bit_slice.chunks::<u8>();

        let result = chunks.interpret().collect::<Vec<_>>();
        assert_eq!(buffer_slice.to_vec(), result);
    }
}