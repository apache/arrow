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
package driver

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
)

// *** GRPC helpers ***
type grpcCredentials struct {
	username string
	password string
	token    string
	params   map[string]string
}

func (g grpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	md := make(map[string]string, len(g.params)+1)

	// Authentication parameters
	switch {
	case g.token != "":
		md["authorization"] = "Bearer " + g.token
	case g.username != "":

		md["authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(g.username+":"+g.password))
	}

	for k, v := range g.params {
		md[k] = v
	}

	return md, nil
}

func (g grpcCredentials) RequireTransportSecurity() bool {
	return g.token != "" || g.username != ""
}

// *** Type conversions ***
func fromArrowType(arr arrow.Array, idx int) (interface{}, error) {
	if arr.IsNull(idx) {
		return nil, nil
	}

	switch c := arr.(type) {
	case *array.Boolean:
		return c.Value(idx), nil
	case *array.Float16:
		return c.Value(idx), nil
	case *array.Float32:
		return c.Value(idx), nil
	case *array.Float64:
		return c.Value(idx), nil
	case *array.Decimal128:
		v := arr.DataType().(*arrow.Decimal128Type)
		return c.Value(idx).ToFloat64(v.Scale), nil
	case *array.Decimal256:
		v := arr.DataType().(*arrow.Decimal256Type)
		return c.Value(idx).ToFloat64(v.Scale), nil
	case *array.Int8:
		return c.Value(idx), nil
	case *array.Int16:
		return c.Value(idx), nil
	case *array.Int32:
		return c.Value(idx), nil
	case *array.Int64:
		return c.Value(idx), nil
	case *array.Binary:
		return c.Value(idx), nil
	case *array.String:
		return c.Value(idx), nil
	case *array.Time32:
		d32 := arr.DataType().(*arrow.Time32Type)
		v := c.Value(idx)
		return v.ToTime(d32.TimeUnit()), nil
	case *array.Time64:
		d64 := arr.DataType().(*arrow.Time64Type)
		v := c.Value(idx)
		return v.ToTime(d64.TimeUnit()), nil
	case *array.Timestamp:
		ts := arr.DataType().(*arrow.TimestampType)
		v := c.Value(idx)
		return v.ToTime(ts.TimeUnit()), nil
	case *array.Date64:
		return c.Value(idx).ToTime(), nil
	case *array.DayTimeInterval:
		durationDays := time.Duration(c.Value(idx).Days*24) * time.Hour
		duration := time.Duration(c.Value(idx).Milliseconds) * time.Millisecond

		return durationDays + duration, nil
	}

	return nil, fmt.Errorf("type %T: %w", arr, ErrNotSupported)
}

func toArrowDataType(value interface{}) (arrow.DataType, error) {
	switch value.(type) {
	case bool:
		return &arrow.BooleanType{}, nil
	case float32:
		return &arrow.Float32Type{}, nil
	case float64:
		return &arrow.Float64Type{}, nil
	case int8:
		return &arrow.Int8Type{}, nil
	case int16:
		return &arrow.Int16Type{}, nil
	case int32:
		return &arrow.Int32Type{}, nil
	case int64:
		return &arrow.Int64Type{}, nil
	case uint8:
		return &arrow.Uint8Type{}, nil
	case uint16:
		return &arrow.Uint16Type{}, nil
	case uint32:
		return &arrow.Uint32Type{}, nil
	case uint64:
		return &arrow.Uint64Type{}, nil
	case string:
		return &arrow.StringType{}, nil
	case time.Time:
		return &arrow.Time64Type{Unit: arrow.Nanosecond}, nil
	}
	return nil, fmt.Errorf("type %T: %w", value, ErrNotSupported)
}

// *** Field builder versions ***
func setFieldValue(builder array.Builder, arg interface{}) error {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		switch v := arg.(type) {
		case bool:
			b.Append(v)
		case []bool:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Float32Builder:
		switch v := arg.(type) {
		case float32:
			b.Append(v)
		case []float32:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Float64Builder:
		switch v := arg.(type) {
		case float64:
			b.Append(v)
		case []float64:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Int8Builder:
		switch v := arg.(type) {
		case int8:
			b.Append(v)
		case []int8:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Int16Builder:
		switch v := arg.(type) {
		case int16:
			b.Append(v)
		case []int16:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Int32Builder:
		switch v := arg.(type) {
		case int32:
			b.Append(v)
		case []int32:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Int64Builder:
		switch v := arg.(type) {
		case int64:
			b.Append(v)
		case []int64:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Uint8Builder:
		switch v := arg.(type) {
		case uint8:
			b.Append(v)
		case []uint8:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Uint16Builder:
		switch v := arg.(type) {
		case uint16:
			b.Append(v)
		case []uint16:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Uint32Builder:
		switch v := arg.(type) {
		case uint32:
			b.Append(v)
		case []uint32:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Uint64Builder:
		switch v := arg.(type) {
		case uint64:
			b.Append(v)
		case []uint64:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.StringBuilder:
		switch v := arg.(type) {
		case string:
			b.Append(v)
		case []string:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Time64Builder:
		switch v := arg.(type) {
		case int64:
			b.Append(arrow.Time64(v))
		case []int64:
			for _, x := range v {
				b.Append(arrow.Time64(x))
			}
		case uint64:
			b.Append(arrow.Time64(v))
		case []uint64:
			for _, x := range v {
				b.Append(arrow.Time64(x))
			}
		case time.Time:
			b.Append(arrow.Time64(v.Nanosecond()))
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	default:
		return fmt.Errorf("unknown builder type %T", builder)
	}
	return nil
}
