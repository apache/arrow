package tools

func Bools(v ...int) []bool {
	res := make([]bool, len(v))
	for i, b := range v {
		res[i] = b != 0
	}
	return res
}
