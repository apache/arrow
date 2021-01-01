/// Converts numeric type to a `String`
pub fn lexical_to_string<N: lexical_core::ToLexical>(n: N) -> String {
    let mut buf = Vec::<u8>::with_capacity(N::FORMATTED_SIZE_DECIMAL);
    unsafe {
        // JUSTIFICATION
        //  Benefit
        //      Allows using the faster serializer lexical core and convert to string
        //  Soundness
        //      Length of buf is set as written length afterwards. lexical_core
        //      creates a valid string, so doesn't need to be checked.
        let slice = std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.capacity());
        let len = lexical_core::write(n, slice).len();
        buf.set_len(len);
        String::from_utf8_unchecked(buf)
    }
}
