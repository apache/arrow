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

package main

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/docopt/docopt-go"
)

const usage = `Parquet Schema Dumper.
Usage:
  parquet_schema -h | --help
  parquet_schema <file>
Options:
  -h --help   Show this screen.`

func main() {
	args, _ := docopt.ParseDoc(usage)
	rdr, err := file.OpenParquetFile(args["<file>"].(string), false)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error opening parquet file: ", err)
		os.Exit(1)
	}

	schema.PrintSchema(rdr.MetaData().Schema.Root(), os.Stdout, 2)
}
