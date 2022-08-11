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

package example

import (
	"database/sql"
)

// func GetColumnMetadata(coltype int, table string) flightsql.ColumnMetadata {}

type Statement struct {
	stmt *sql.Stmt
	db   *sql.DB
}

func NewStatement(db *sql.DB, query string) (*Statement, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Statement{stmt, db}, nil
}
