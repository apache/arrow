package arrow

type BinaryType struct{}

func (t *BinaryType) ID() Type     { return BINARY }
func (t *BinaryType) Name() string { return "binary" }
func (t *BinaryType) binary()      {}

type StringType struct{}

func (t *StringType) ID() Type     { return STRING }
func (t *StringType) Name() string { return "utf8" }
func (t *StringType) binary()      {}

var (
	BinaryTypes = struct {
		Binary BinaryDataType
		String BinaryDataType
	}{
		Binary: &BinaryType{},
		String: &StringType{},
	}
)
