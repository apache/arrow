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

package main // import "github.com/apache/arrow/go/arrow/ipc/cmd/arrow-json-integration-test"

import (
	"flag"
	"log"
	"os"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/arrio"
	"github.com/apache/arrow/go/arrow/internal/arrjson"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/pkg/errors"
)

func main() {
	log.SetPrefix("arrow-json: ")
	log.SetFlags(0)

	var (
		arrowPath = flag.String("arrow", "", "path to ARROW file")
		jsonPath  = flag.String("json", "", "path to JSON file")
		mode      = flag.String("mode", "VALIDATE", "mode of integration testing tool (ARROW_TO_JSON, JSON_TO_ARROW, VALIDATE)")
		verbose   = flag.Bool("verbose", true, "enable/disable verbose mode")
	)

	flag.Parse()

	err := runCommand(*jsonPath, *arrowPath, *mode, *verbose)
	if err != nil {
		log.Fatal(err)
	}
}

func runCommand(jsonName, arrowName, mode string, verbose bool) error {
	if jsonName == "" {
		return errors.Errorf("must specify json file name")
	}

	if arrowName == "" {
		return errors.Errorf("must specify arrow file name")
	}

	switch mode {
	case "ARROW_TO_JSON":
		return cnvToJSON(arrowName, jsonName, verbose)
	case "JSON_TO_ARROW":
		return cnvToARROW(arrowName, jsonName, verbose)
	case "VALIDATE":
		return validate(arrowName, jsonName, verbose)
	default:
		return errors.Errorf("unknown command %q", mode)
	}

	return nil
}

func cnvToJSON(arrowName, jsonName string, verbose bool) error {
	r, err := os.Open(arrowName)
	if err != nil {
		return errors.Wrapf(err, "could not open ARROW file %q", arrowName)
	}
	defer r.Close()

	w, err := os.Create(jsonName)
	if err != nil {
		return errors.Wrapf(err, "could not create JSON file %q", jsonName)
	}
	defer w.Close()

	rr, err := ipc.NewFileReader(r)
	if err != nil {
		return errors.Wrapf(err, "could not open ARROW file reader from file %q", arrowName)
	}
	defer rr.Close()

	if verbose {
		log.Printf("found schema:\n%v\n", rr.Schema())
	}

	ww, err := arrjson.NewWriter(w, rr.Schema())
	if err != nil {
		return errors.Wrap(err, "could not create JSON encoder")
	}
	defer ww.Close()

	n, err := arrio.Copy(ww, rr)
	if err != nil {
		return errors.Wrap(err, "could not convert ARROW file reader data to JSON data")
	}

	if got, want := n, int64(rr.NumRecords()); got != want {
		return errors.Errorf("invalid number of records copied (got=%d, want=%d", got, want)
	}

	err = ww.Close()
	if err != nil {
		return errors.Wrapf(err, "could not close JSON encoder %q", jsonName)
	}

	err = w.Close()
	if err != nil {
		return errors.Wrapf(err, "could not close JSON file %q", jsonName)
	}

	return nil
}

func cnvToARROW(arrowName, jsonName string, verbose bool) error {
	r, err := os.Open(jsonName)
	if err != nil {
		return errors.Wrapf(err, "could not open JSON file %q", jsonName)
	}
	defer r.Close()

	w, err := os.Create(arrowName)
	if err != nil {
		return errors.Wrapf(err, "could not create ARROW file %q", arrowName)
	}
	defer w.Close()

	rr, err := arrjson.NewReader(r)
	if err != nil {
		return errors.Wrapf(err, "could not open JSON file reader from file %q", jsonName)
	}

	if verbose {
		log.Printf("found schema:\n%v\n", rr.Schema())
	}

	ww, err := ipc.NewFileWriter(w, ipc.WithSchema(rr.Schema()))
	if err != nil {
		return errors.Wrap(err, "could not create ARROW file writer")
	}
	defer ww.Close()

	n, err := arrio.Copy(ww, rr)
	if err != nil {
		return errors.Wrap(err, "could not convert JSON data to ARROW data")
	}

	if got, want := n, int64(rr.NumRecords()); got != want {
		return errors.Errorf("invalid number of records copied (got=%d, want=%d", got, want)
	}

	err = ww.Close()
	if err != nil {
		return errors.Wrapf(err, "could not close ARROW file writer %q", arrowName)
	}

	err = w.Close()
	if err != nil {
		return errors.Wrapf(err, "could not close ARROW file %q", arrowName)
	}

	return nil
}

func validate(arrowName, jsonName string, verbose bool) error {
	jr, err := os.Open(jsonName)
	if err != nil {
		return errors.Wrapf(err, "could not open JSON file %q", jsonName)
	}
	defer jr.Close()

	jrr, err := arrjson.NewReader(jr)
	if err != nil {
		return errors.Wrapf(err, "could not open JSON file reader from file %q", jsonName)
	}

	ar, err := os.Open(arrowName)
	if err != nil {
		return errors.Wrapf(err, "could not open ARROW file %q", arrowName)
	}
	defer ar.Close()

	arr, err := ipc.NewFileReader(ar)
	if err != nil {
		return errors.Wrapf(err, "could not open ARROW file reader from file %q", arrowName)
	}
	defer arr.Close()

	if !arr.Schema().Equal(jrr.Schema()) {
		if verbose {
			log.Printf("JSON schema:\n%v\nArrow schema:\n%v\n", arr.Schema(), jrr.Schema())
		}
		return errors.Errorf("schemas did not match")
	}

	for i := 0; i < arr.NumRecords(); i++ {
		arec, err := arr.Read()
		if err != nil {
			return errors.Wrapf(err, "could not read record %d from ARROW file", i)
		}
		jrec, err := jrr.Read()
		if err != nil {
			return errors.Wrapf(err, "could not read record %d from JSON file", i)
		}
		if !array.RecordApproxEqual(jrec, arec) {
			return errors.Errorf("record batch %d did not match\nJSON:\n%v\nARROW:\n%v\n",
				i, jrec, arec,
			)
		}
	}

	if jn, an := jrr.NumRecords(), arr.NumRecords(); jn != an {
		return errors.Errorf("different number of record batches: %d (JSON) vs %d (Arrow)", jn, an)
	}

	return nil
}
