// +build noasm

package memory

func init() {
	memset = memory_memset_go
}
