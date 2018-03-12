package memory

var (
	memset func(b []byte, c byte)
)

// Set assigns the value c to every element of the slice buf.
func Set(buf []byte, c byte) {
	memset(buf, c)
}

// memory_memset_go reference implementation
func memory_memset_go(buf []byte, c byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = c
	}
}
