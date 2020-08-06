// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL Parser
//!
//! Declares a SQL parser based on sqlparser that handles custom formats that we need.

use sqlparser::{
    ast::{ColumnDef, Statement as SQLStatement, TableConstraint},
    dialect::{keywords::Keyword, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

/// Types of files to parse as DataFrames
#[derive(Debug, Clone, PartialEq)]
pub enum FileType {
    /// Newline-delimited JSON
    NdJson,
    /// Apache Parquet columnar storage
    Parquet,
    /// Comma separated values
    CSV,
}

/// DataFusion Statement representations.
///
/// Tokens parsed by `DFParser` are converted into these values.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// ANSI SQL AST node
    Statement(SQLStatement),
    /// DDL for creating an external table in DataFusion
    CreateExternalTable {
        /// Table name
        name: String,
        /// Optional schema
        columns: Vec<ColumnDef>,
        /// File type (Parquet, NDJSON, CSV)
        file_type: FileType,
        /// CSV Header row?
        has_header: bool,
        /// Path to file
        location: String,
    },
}

/// SQL Parser
pub struct DFParser {
    parser: Parser,
}

impl DFParser {
    /// Parse the specified tokens
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize()?;
        Ok(DFParser {
            parser: Parser::new(tokens),
        })
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let mut tokenizer = Tokenizer::new(&GenericDialect {}, &sql);
        tokenizer.tokenize()?;
        let mut parser = DFParser::new(sql)?;
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report unexpected token
    fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => match w.keyword {
                Keyword::CREATE => {
                    // move one token forward
                    self.parser.next_token();
                    // use custom parsing
                    Ok(self.parse_create()?)
                }
                _ => {
                    // use the native parser
                    Ok(Statement::Statement(self.parser.parse_statement()?))
                }
            },
            _ => {
                // use the native parser
                Ok(Statement::Statement(self.parser.parse_statement()?))
            }
        }
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        if self.parser.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table()
        } else {
            Ok(Statement::Statement(self.parser.parse_create()?))
        }
    }

    // This is a copy of the equivalent implementation in sqlparser.
    fn parse_columns(
        &mut self,
    ) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen)
            || self.parser.consume_token(&Token::RParen)
        {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token() {
                let column_def = self.parse_column_def()?;
                columns.push(column_def);
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parser.parse_identifier()?;
        let data_type = self.parser.parse_data_type()?;
        let collation = if self.parser.parse_keyword(Keyword::COLLATE) {
            Some(self.parser.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            match self.parser.peek_token() {
                Token::EOF | Token::Comma | Token::RParen => break,
                _ => options.push(self.parser.parse_column_option_def()?),
            }
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    fn parse_create_external_table(&mut self) -> Result<Statement, ParserError> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let table_name = self.parser.parse_object_name()?;
        let (columns, _) = self.parse_columns()?;
        self.parser
            .expect_keywords(&[Keyword::STORED, Keyword::AS])?;

        // THIS is the main difference: we parse a different file format.
        let file_type = self.parse_file_format()?;

        let has_header = self.parse_csv_has_header();

        self.parser.expect_keyword(Keyword::LOCATION)?;
        let location = self.parser.parse_literal_string()?;

        Ok(Statement::CreateExternalTable {
            name: table_name.to_string(),
            columns,
            file_type,
            has_header,
            location,
        })
    }

    /// Parses the set of valid formats
    fn parse_file_format(&mut self) -> Result<FileType, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match &*w.value {
                "PARQUET" => Ok(FileType::Parquet),
                "NDJSON" => Ok(FileType::NdJson),
                "CSV" => Ok(FileType::CSV),
                _ => self.expected("one of PARQUET, NDJSON, or CSV", Token::Word(w)),
            },
            unexpected => self.expected("one of PARQUET, NDJSON, or CSV", unexpected),
        }
    }

    fn consume_token(&mut self, expected: &str) -> bool {
        if self.parser.peek_token().to_string() == *expected {
            self.parser.next_token();
            true
        } else {
            false
        }
    }

    fn parse_csv_has_header(&mut self) -> bool {
        self.consume_token("WITH")
            & self.consume_token("HEADER")
            & self.consume_token("ROW")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{DataType, Ident};

    fn expect_parse_ok(sql: &str, expected: Statement) -> Result<(), ParserError> {
        let statements = DFParser::parse_sql(sql)?;
        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        assert_eq!(statements[0], expected);
        Ok(())
    }

    /// Parses sql and asserts that the expected error message was found
    fn expect_parse_error(sql: &str, expected_error: &str) -> Result<(), ParserError> {
        match DFParser::parse_sql(sql) {
            Ok(statements) => {
                assert!(
                    false,
                    "Expected parse error for '{}', but was successful: {:?}",
                    sql, statements
                );
            }
            Err(e) => {
                let error_message = e.to_string();
                assert!(
                    error_message.contains(expected_error),
                    "Expected error '{}' not found in actual error '{}'",
                    expected_error,
                    error_message
                );
            }
        }
        Ok(())
    }

    fn make_column_def(name: impl Into<String>, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident {
                value: name.into(),
                quote_style: None,
            },
            data_type,
            collation: None,
            options: vec![],
        }
    }

    #[test]
    fn create_external_table() -> Result<(), ParserError> {
        // positive case
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'";
        let expected = Statement::CreateExternalTable {
            name: "t".into(),
            columns: vec![make_column_def("c1", DataType::Int)],
            file_type: FileType::CSV,
            has_header: false,
            location: "foo.csv".into(),
        };
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for parquet files not to have columns specified
        let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable {
            name: "t".into(),
            columns: vec![],
            file_type: FileType::Parquet,
            has_header: false,
            location: "foo.parquet".into(),
        };
        expect_parse_ok(sql, expected)?;

        // Error cases: Invalid type
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS UNKNOWN_TYPE LOCATION 'foo.csv'";
        expect_parse_error(
            sql,
            "Expected one of PARQUET, NDJSON, or CSV, found: UNKNOWN_TYPE",
        )?;

        Ok(())
    }
}
