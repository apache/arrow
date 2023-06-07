package arrow

func ReleaseArrays(arrays []Array) {
	for _, a := range arrays {
		a.Release()
	}
}

func ReleaseArrayData(data []ArrayData) {
	for _, d := range data {
		d.Release()
	}
}
