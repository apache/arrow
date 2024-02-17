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
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/apache/arrow/go/v16/arrow/avro"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/compress"
	pq "github.com/apache/arrow/go/v16/parquet/pqarrow"
)

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	filepath   = flag.String("file", "", "avro ocf to convert")
)

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	if *filepath == "" {
		fmt.Println("no file specified")
	}
	chunk := 1024 * 8
	ts := time.Now()
	log.Println("starting:")
	info, err := os.Stat(*filepath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	filesize := info.Size()
	data, err := os.ReadFile(*filepath)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	fmt.Printf("file : %v\nsize: %v MB\n", filepath, float64(filesize)/1024/1024)

	r := bytes.NewReader(data)
	ior := bufio.NewReaderSize(r, 4096*8)
	av2arReader, err := avro.NewOCFReader(ior, avro.WithChunk(chunk))
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}
	fp, err := os.OpenFile(*filepath+".parquet", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Println(err)
		os.Exit(4)
	}
	defer fp.Close()
	pwProperties := parquet.NewWriterProperties(parquet.WithDictionaryDefault(true),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithBatchSize(1024*32),
		parquet.WithDataPageSize(1024*1024),
		parquet.WithMaxRowGroupLength(64*1024*1024),
	)
	awProperties := pq.NewArrowWriterProperties(pq.WithStoreSchema())
	pr, err := pq.NewFileWriter(av2arReader.Schema(), fp, pwProperties, awProperties)
	if err != nil {
		fmt.Println(err)
		os.Exit(5)
	}
	defer pr.Close()
	fmt.Printf("parquet version: %v\n", pwProperties.Version())
	for av2arReader.Next() {
		if av2arReader.Err() != nil {
			fmt.Println(err)
			os.Exit(6)
		}
		recs := av2arReader.Record()
		err = pr.WriteBuffered(recs)
		if err != nil {
			fmt.Println(err)
			os.Exit(7)
		}
		recs.Release()
	}
	if av2arReader.Err() != nil {
		fmt.Println(av2arReader.Err())
	}

	pr.Close()
	log.Printf("time to convert: %v\n", time.Since(ts))
}
