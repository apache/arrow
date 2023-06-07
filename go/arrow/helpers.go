package arrow

func ReleaseArrays(arrays ...Array) {
	for _, arr := range arrays {
		arr.Release()
	}
}
