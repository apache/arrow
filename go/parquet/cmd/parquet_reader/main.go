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
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v9/parquet/file"
	"github.com/apache/arrow/go/v9/parquet/metadata"
	"github.com/apache/arrow/go/v9/parquet/schema"
	"github.com/docopt/docopt-go"
)

const usage = `Parquet Reader.
Usage:
  parquet_reader -h | --help
  parquet_reader [--only-metadata] [--no-memory-map] [--json]
                 [--print-key-value-metadata] [--columns=COLUMNS] <file>
Options:
  -h --help                     Show this screen.
  --print-key-value-metadata    Print out the key-value metadata [default: false]
  --only-metadata               Stop after printing metadata, no values.
  --no-memory-map               Disable memory mapping the file.
  --json                        Format output as JSON instead of text.
  --columns=COLUMNS             Specify a subset of columns to print, comma delimited indexes.`

func main() {
	opts, _ := docopt.ParseDoc(usage)
	var config struct {
		PrintKeyValueMetadata bool
		OnlyMetadata          bool
		NoMemoryMap           bool
		JSON                  bool `docopt:"--json"`
		Columns               string
		File                  string
	}
	opts.Bind(&config)

	if config.JSON {
		fmt.Fprintln(os.Stderr, "error: json output not implemented yet! falling back to regular")
	}

	selectedColumns := []int{}
	if config.Columns != "" {
		for _, c := range strings.Split(config.Columns, ",") {
			cval, err := strconv.Atoi(c)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error: --columns needs to be comma-delimited integers")
				os.Exit(1)
			}
			selectedColumns = append(selectedColumns, cval)
		}
	}

	rdr, err := file.OpenParquetFile(config.File, !config.NoMemoryMap)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error opening parquet file: ", err)
		os.Exit(1)
	}

	fileMetadata := rdr.MetaData()

	fmt.Println("File name:", config.File)
	fmt.Println("Version:", fileMetadata.Version())
	fmt.Println("Created By:", fileMetadata.GetCreatedBy())
	fmt.Println("Num Rows:", rdr.NumRows())

	keyvaluemeta := fileMetadata.KeyValueMetadata()
	if config.PrintKeyValueMetadata && keyvaluemeta != nil {
		fmt.Println("Key Value File Metadata:", keyvaluemeta.Len(), "entries")
		keys := keyvaluemeta.Keys()
		values := keyvaluemeta.Values()
		for i := 0; i < keyvaluemeta.Len(); i++ {
			fmt.Printf("Key nr %d %s: %s\n", i, keys[i], values[i])
		}
	}

	fmt.Println("Number of RowGroups:", rdr.NumRowGroups())
	fmt.Println("Number of Real Columns:", fileMetadata.Schema.Root().NumFields())
	fmt.Println("Number of Columns:", fileMetadata.Schema.NumColumns())

	if len(selectedColumns) == 0 {
		for i := 0; i < fileMetadata.Schema.NumColumns(); i++ {
			selectedColumns = append(selectedColumns, i)
		}
	} else {
		for _, c := range selectedColumns {
			if c < 0 || c >= fileMetadata.Schema.NumColumns() {
				fmt.Fprintln(os.Stderr, "selected column is out of range")
				os.Exit(1)
			}
		}
	}

	fmt.Println("Number of Selected Columns:", len(selectedColumns))
	for _, c := range selectedColumns {
		descr := fileMetadata.Schema.Column(c)
		fmt.Printf("Column %d: %s (%s", c, descr.Path(), descr.PhysicalType())
		if descr.ConvertedType() != schema.ConvertedTypes.None {
			fmt.Printf("/%s", descr.ConvertedType())
			if descr.ConvertedType() == schema.ConvertedTypes.Decimal {
				dec := descr.LogicalType().(*schema.DecimalLogicalType)
				fmt.Printf("(%d,%d)", dec.Precision(), dec.Scale())
			}
		}
		fmt.Print(")\n")
	}

	for r := 0; r < rdr.NumRowGroups(); r++ {
		fmt.Println("--- Row Group:", r, " ---")

		rgr := rdr.RowGroup(r)
		rowGroupMeta := rgr.MetaData()
		fmt.Println("--- Total Bytes:", rowGroupMeta.TotalByteSize(), " ---")
		fmt.Println("--- Rows:", rgr.NumRows(), " ---")

		for _, c := range selectedColumns {
			chunkMeta, err := rowGroupMeta.ColumnChunk(c)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("Column", c)
			if set, _ := chunkMeta.StatsSet(); set {
				stats, err := chunkMeta.Statistics()
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf(" Values: %d", chunkMeta.NumValues())
				if stats.HasMinMax() {
					fmt.Printf(", Min: %v, Max: %v",
						metadata.GetStatValue(stats.Type(), stats.EncodeMin()),
						metadata.GetStatValue(stats.Type(), stats.EncodeMax()))
				}
				if stats.HasNullCount() {
					fmt.Printf(", Null Values: %d", stats.NullCount())
				}
				if stats.HasDistinctCount() {
					fmt.Printf(", Distinct Values: %d", stats.DistinctCount())
				}
				fmt.Println()
			} else {
				fmt.Println(" Values:", chunkMeta.NumValues(), "Statistics Not Set")
			}

			fmt.Print(" Compression: ", chunkMeta.Compression())
			fmt.Print(", Encodings:")
			for _, enc := range chunkMeta.Encodings() {
				fmt.Print(" ", enc)
			}
			fmt.Println()

			fmt.Print(" Uncompressed Size: ", chunkMeta.TotalUncompressedSize())
			fmt.Println(", Compressed Size:", chunkMeta.TotalCompressedSize())
		}

		if config.OnlyMetadata {
			continue
		}

		fmt.Println("--- Values ---")

		const colwidth = 18

		scanners := make([]*Dumper, len(selectedColumns))
		for idx, c := range selectedColumns {
			scanners[idx] = createDumper(rgr.Column(c))
			fmt.Printf(fmt.Sprintf("%%-%ds|", colwidth), rgr.Column(c).Descriptor().Name())
		}
		fmt.Println()

		for {
			data := false
			for _, s := range scanners {
				if val, ok := s.Next(); ok {
					fmt.Print(s.FormatValue(val, colwidth), "|")
					data = true
				} else {
					fmt.Printf(fmt.Sprintf("%%-%ds|", colwidth), "")
				}
			}
			fmt.Println()
			if !data {
				break
			}
		}
		fmt.Println()
	}
}
