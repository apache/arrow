package memory

func ReleaseBuffers(buffers []*Buffer) {
	for _, buff := range buffers {
		if buff != nil {
			buff.Release()
		}
	}
}
