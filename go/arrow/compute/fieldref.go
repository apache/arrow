// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compute

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math/bits"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
)

var (
	ErrEmpty           = errors.New("cannot traverse empty field path")
	ErrNoChildren      = errors.New("trying to get child of type with no children")
	ErrIndexRange      = errors.New("index out of range")
	ErrMultipleMatches = errors.New("multiple matches")
	ErrNoMatch         = errors.New("no match")
	ErrInvalid         = errors.New("field ref invalid")
)

func getFields(typ arrow.DataType) []arrow.Field {
	if nested, ok := typ.(arrow.NestedType); ok {
		return nested.Fields()
	}
	return nil
}

type listvals interface {
	ListValues() array.Interface
}

func getChildren(arr array.Interface) (ret []array.Interface) {
	switch arr := arr.(type) {
	case *array.Struct:
		ret = make([]array.Interface, arr.NumField())
		for i := 0; i < arr.NumField(); i++ {
			ret[i] = arr.Field(i)
		}
	case listvals:
		ret = []array.Interface{arr.ListValues()}
	}
	return
}

type FieldPath []int

func (f FieldPath) String() string {
	if len(f) == 0 {
		return "FieldPath(empty)"
	}

	var b strings.Builder
	b.WriteString("FieldPath(")
	for _, i := range f {
		fmt.Fprint(&b, i)
		b.WriteByte(' ')
	}
	ret := b.String()
	return ret[:len(ret)-1] + ")"
}

func (f FieldPath) Get(s *arrow.Schema) (*arrow.Field, error) {
	return f.GetFieldFromSlice(s.Fields())
}

func (f FieldPath) GetFieldFromSlice(fields []arrow.Field) (*arrow.Field, error) {
	if len(f) == 0 {
		return nil, ErrEmpty
	}

	var (
		depth = 0
		out   *arrow.Field
	)
	for _, idx := range f {
		if len(fields) == 0 {
			return nil, fmt.Errorf("%w: %s", ErrNoChildren, out.Type)
		}

		if idx < 0 || idx >= len(fields) {
			return nil, fmt.Errorf("%w: indices=%s", ErrIndexRange, f[:depth+1])
		}

		out = &fields[idx]
		fields = getFields(out.Type)
		depth++
	}

	return out, nil
}

func (f FieldPath) getArray(arrs []array.Interface) (array.Interface, error) {
	if len(f) == 0 {
		return nil, ErrEmpty
	}

	var (
		depth = 0
		out   array.Interface
	)
	for _, idx := range f {
		if len(arrs) == 0 {
			return nil, fmt.Errorf("%w: %s", ErrNoChildren, out.DataType())
		}

		if idx < 0 || idx >= len(arrs) {
			return nil, fmt.Errorf("%w. indices=%s", ErrIndexRange, f[:depth+1])
		}

		out = arrs[idx]
		arrs = getChildren(out)
		depth++
	}
	return out, nil
}

func (f FieldPath) GetFieldFromType(typ arrow.DataType) (*arrow.Field, error) {
	return f.GetFieldFromSlice(getFields(typ))
}

func (f FieldPath) GetField(field arrow.Field) (*arrow.Field, error) {
	return f.GetFieldFromType(field.Type)
}

func (f FieldPath) GetColumn(batch array.Record) (array.Interface, error) {
	return f.getArray(batch.Columns())
}

func (f FieldPath) hash(h *maphash.Hash) {
	raw := (*reflect.SliceHeader)(unsafe.Pointer(&f)).Data

	var b []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	s.Data = raw
	if bits.UintSize == 32 {
		s.Len = arrow.Int32Traits.BytesRequired(len(f))
	} else {
		s.Len = arrow.Int64Traits.BytesRequired(len(f))
	}
	s.Cap = s.Len
	h.Write(b)
}

func (f FieldPath) findAll(fields []arrow.Field) []FieldPath {
	_, err := f.GetFieldFromSlice(fields)
	if err == nil {
		return []FieldPath{f}
	}
	return nil
}

type nameRef string

func (ref nameRef) findAll(fields []arrow.Field) []FieldPath {
	out := []FieldPath{}
	for i, f := range fields {
		if f.Name == string(ref) {
			out = append(out, FieldPath{i})
		}
	}
	return out
}

func (ref nameRef) hash(h *maphash.Hash) { h.WriteString(string(ref)) }

type matches struct {
	prefixes []FieldPath
	refs     []*arrow.Field
}

func (m *matches) add(prefix, suffix FieldPath, fields []arrow.Field) {
	f, err := suffix.GetFieldFromSlice(fields)
	if err != nil {
		panic(err)
	}

	m.refs = append(m.refs, f)
	m.prefixes = append(m.prefixes, append(prefix, suffix...))
}

type refList []FieldRef

func (ref refList) hash(h *maphash.Hash) {
	for _, r := range ref {
		r.hash(h)
	}
}

func (ref refList) findAll(fields []arrow.Field) []FieldPath {
	if len(ref) == 0 {
		return nil
	}

	m := matches{}
	for _, list := range ref[0].FindAll(fields) {
		m.add(FieldPath{}, list, fields)
	}

	for _, r := range ref[1:] {
		next := matches{}
		for i, f := range m.refs {
			for _, match := range r.FindAllField(*f) {
				next.add(m.prefixes[i], match, getFields(f.Type))
			}
		}
		m = next
	}
	return m.prefixes
}

type refImpl interface {
	findAll(fields []arrow.Field) []FieldPath
	hash(h *maphash.Hash)
}

type FieldRef struct {
	impl refImpl
}

func FieldRefPath(p FieldPath) FieldRef {
	return FieldRef{impl: p}
}

func FieldRefIndex(i int) FieldRef {
	return FieldRef{impl: FieldPath{i}}
}

func FieldRefName(n string) FieldRef {
	return FieldRef{impl: nameRef(n)}
}

// FieldRefList takes an arbitrary number of arguments which can be either
// strings or ints. This will panic if anything other than a string or int
// is passed in.
func FieldRefList(elems ...interface{}) FieldRef {
	list := make(refList, len(elems))
	for i, e := range elems {
		switch e := e.(type) {
		case string:
			list[i] = FieldRefName(e)
		case int:
			list[i] = FieldRefIndex(e)
		}
	}
	return FieldRef{impl: list}
}

func NewFieldRefFromDotPath(dotpath string) (out FieldRef, err error) {
	if len(dotpath) == 0 {
		return out, fmt.Errorf("%w dotpath was empty", ErrInvalid)
	}

	parseName := func() string {
		var name string
		for {
			idx := strings.IndexAny(dotpath, `\[.`)
			if idx == -1 {
				name += dotpath
				dotpath = ""
				break
			}

			if dotpath[idx] != '\\' {
				// subscript for a new field ref
				name += dotpath[:idx]
				dotpath = dotpath[idx:]
				break
			}

			if len(dotpath) == idx+1 {
				// dotpath ends with a backslash; consume it all
				name += dotpath
				dotpath = ""
				break
			}

			// append all characters before backslash, then the character which follows it
			name += dotpath[:idx] + string(dotpath[idx+1])
			dotpath = dotpath[idx+2:]
		}
		return name
	}

	children := make([]FieldRef, 0)

	for len(dotpath) > 0 {
		subscript := dotpath[0]
		dotpath = dotpath[1:]
		switch subscript {
		case '.':
			// next element is a name
			children = append(children, FieldRef{nameRef(parseName())})
		case '[':
			subend := strings.IndexFunc(dotpath, func(r rune) bool { return !unicode.IsDigit(r) })
			if subend == -1 || dotpath[subend] != ']' {
				return out, fmt.Errorf("%w: dot path '%s' contained an unterminated index", ErrInvalid, dotpath)
			}
			idx, _ := strconv.Atoi(dotpath[:subend])
			children = append(children, FieldRef{FieldPath{idx}})
			dotpath = dotpath[subend+1:]
		default:
			return out, fmt.Errorf("%w: dot path must begin with '[' or '.' got '%s'", ErrInvalid, dotpath)
		}
	}

	out.flatten(children)
	return
}

func (f FieldRef) hash(h *maphash.Hash) { f.impl.hash(h) }

func (f FieldRef) Hash(seed maphash.Seed) uint64 {
	h := maphash.Hash{}
	h.SetSeed(seed)
	f.hash(&h)
	return h.Sum64()
}

func (f *FieldRef) IsName() bool {
	_, ok := f.impl.(nameRef)
	return ok
}

func (f *FieldRef) IsFieldPath() bool {
	_, ok := f.impl.(FieldPath)
	return ok
}

func (f *FieldRef) IsNested() bool {
	switch impl := f.impl.(type) {
	case nameRef:
		return false
	case FieldPath:
		return len(impl) > 1
	default:
		return true
	}
}

func (f *FieldRef) Name() string {
	n, _ := f.impl.(nameRef)
	return string(n)
}

func (f *FieldRef) FieldPath() FieldPath {
	p, _ := f.impl.(FieldPath)
	return p
}

func (f *FieldRef) Equals(other FieldRef) bool {
	return reflect.DeepEqual(f.impl, other.impl)
}

func (f *FieldRef) flatten(children []FieldRef) {
	out := make([]FieldRef, 0, len(children))

	var populate func(refImpl)
	populate = func(refs refImpl) {
		switch r := refs.(type) {
		case nameRef:
			out = append(out, FieldRef{r})
		case FieldPath:
			out = append(out, FieldRef{r})
		case refList:
			for _, c := range r {
				populate(c.impl)
			}
		}
	}

	populate(refList(children))

	if len(out) == 1 {
		f.impl = out[0].impl
	} else {
		f.impl = refList(out)
	}
}

func (f FieldRef) FindAll(fields []arrow.Field) []FieldPath {
	return f.impl.findAll(fields)
}

func (f FieldRef) FindAllField(field arrow.Field) []FieldPath {
	return f.impl.findAll(getFields(field.Type))
}

func (f FieldRef) FindOneOrNone(schema *arrow.Schema) (FieldPath, error) {
	matches := f.FindAll(schema.Fields())
	if len(matches) > 1 {
		return nil, fmt.Errorf("%w for %s in %s", ErrMultipleMatches, f, schema)
	}
	if len(matches) == 0 {
		return nil, nil
	}
	return matches[0], nil
}

func (f FieldRef) FindOneOrNoneRecord(root array.Record) (FieldPath, error) {
	return f.FindOneOrNone(root.Schema())
}

func (f FieldRef) FindOne(schema *arrow.Schema) (FieldPath, error) {
	matches := f.FindAll(schema.Fields())
	if len(matches) == 0 {
		return nil, fmt.Errorf("%w for %s in %s", ErrNoMatch, f, schema)
	}
	if len(matches) > 1 {
		return nil, fmt.Errorf("%w for %s in %s", ErrMultipleMatches, f, schema)
	}
	return matches[0], nil
}

func (f FieldRef) GetAllColumns(root array.Record) ([]array.Interface, error) {
	out := make([]array.Interface, 0)
	for _, m := range f.FindAll(root.Schema().Fields()) {
		n, err := m.GetColumn(root)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func (f FieldRef) GetOneField(schema *arrow.Schema) (*arrow.Field, error) {
	match, err := f.FindOne(schema)
	if err != nil {
		return nil, err
	}

	return match.GetFieldFromSlice(schema.Fields())
}

func (f FieldRef) GetOneOrNone(schema *arrow.Schema) (*arrow.Field, error) {
	match, err := f.FindOneOrNone(schema)
	if err != nil {
		return nil, err
	}
	if len(match) == 0 {
		return nil, nil
	}
	return match.GetFieldFromSlice(schema.Fields())
}

func (f FieldRef) GetOneColumnOrNone(root array.Record) (array.Interface, error) {
	match, err := f.FindOneOrNoneRecord(root)
	if err != nil {
		return nil, err
	}
	if len(match) == 0 {
		return nil, nil
	}
	return match.GetColumn(root)
}
