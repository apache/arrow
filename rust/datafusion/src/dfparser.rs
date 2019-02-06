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
//! Note that most SQL parsing is now delegated to the sqlparser crate, which handles ANSI SQL but
//! this module contains DataFusion-specific SQL extensions.

use sqlparser::dialect::*;
use sqlparser::sqlast::*;
use sqlparser::sqlparser::*;
use sqlparser::sqltokenizer::*;

macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

#[derive(Debug, Clone)]
pub enum FileType {
    NdJson,
    Parquet,
    CSV,
}

#[derive(Debug, Clone)]
pub enum DFASTNode {
    /// ANSI SQL AST node
    ANSI(ASTNode),
    /// DDL for creating an external table in DataFusion
    CreateExternalTable {
        /// Table name
        name: String,
        /// Optional schema
        columns: Vec<SQLColumnDef>,
        /// File type (Parquet, NDJSON, CSV)
        file_type: FileType,
        /// Header row?
        header_row: bool,
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
    pub fn new(sql: String) -> Result<Self, ParserError> {
        let dialect = GenericSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, &sql);
        let tokens = tokenizer.tokenize()?;
        Ok(DFParser {
            parser: Parser::new(tokens),
        })
    }

    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql(sql: String) -> Result<DFASTNode, ParserError> {
        let mut parser = DFParser::new(sql)?;
        parser.parse()
    }

    /// Parse a new expression
    pub fn parse(&mut self) -> Result<DFASTNode, ParserError> {
        self.parse_expr(0)
    }

    /// Parse tokens until the precedence changes
    fn parse_expr(&mut self, precedence: u8) -> Result<DFASTNode, ParserError> {
        let mut expr = self.parse_prefix()?;
        loop {
            let next_precedence = self.parser.get_next_precedence()?;
            if precedence >= next_precedence {
                break;
            }

            if let Some(infix_expr) = self.parse_infix(expr.clone(), next_precedence)? {
                expr = infix_expr;
            }
        }
        Ok(expr)
    }

    /// Parse an expression prefix
    fn parse_prefix(&mut self) -> Result<DFASTNode, ParserError> {
        if self
            .parser
            .parse_keywords(vec!["CREATE", "EXTERNAL", "TABLE"])
        {
            match self.parser.next_token() {
                Some(Token::Identifier(id)) => {
                    // parse optional column list (schema)
                    let mut columns = vec![];
                    if self.parser.consume_token(&Token::LParen) {
                        loop {
                            if let Some(Token::Identifier(column_name)) =
                                self.parser.next_token()
                            {
                                if let Ok(data_type) = self.parser.parse_data_type() {
                                    let allow_null = if self
                                        .parser
                                        .parse_keywords(vec!["NOT", "NULL"])
                                    {
                                        false
                                    } else if self.parser.parse_keyword("NULL") {
                                        true
                                    } else {
                                        true
                                    };

                                    match self.parser.peek_token() {
                                        Some(Token::Comma) => {
                                            self.parser.next_token();
                                            columns.push(SQLColumnDef {
                                                name: column_name,
                                                data_type: data_type,
                                                allow_null,
                                                default: None,
                                                is_primary: false,
                                                is_unique: false,
                                            });
                                        }
                                        Some(Token::RParen) => break,
                                        _ => {
                                            return parser_err!(
                                                "Expected ',' or ')' after column definition"
                                            );
                                        }
                                    }
                                } else {
                                    return parser_err!(
                                        "Error parsing data type in column definition"
                                    );
                                }
                            } else {
                                return parser_err!("Error parsing column name");
                            }
                        }
                    }

                    //println!("Parsed {} column defs", columns.len());

                    let mut headers = true;
                    let file_type: FileType = if self
                        .parser
                        .parse_keywords(vec!["STORED", "AS", "CSV"])
                    {
                        if self.parser.parse_keywords(vec!["WITH", "HEADER", "ROW"]) {
                            headers = true;
                        } else if self
                            .parser
                            .parse_keywords(vec!["WITHOUT", "HEADER", "ROW"])
                        {
                            headers = false;
                        }
                        FileType::CSV
                    } else if self.parser.parse_keywords(vec!["STORED", "AS", "NDJSON"]) {
                        FileType::NdJson
                    } else if self.parser.parse_keywords(vec!["STORED", "AS", "PARQUET"])
                    {
                        FileType::Parquet
                    } else {
                        return parser_err!(format!(
                            "Expected 'STORED AS' clause, found {:?}",
                            self.parser.peek_token()
                        ));
                    };

                    let location: String = if self.parser.parse_keywords(vec!["LOCATION"])
                    {
                        self.parser.parse_literal_string()?
                    } else {
                        return parser_err!("Missing 'LOCATION' clause");
                    };

                    Ok(DFASTNode::CreateExternalTable {
                        name: id,
                        columns,
                        file_type,
                        header_row: headers,
                        location,
                    })
                }
                _ => parser_err!(format!(
                    "Unexpected token after CREATE EXTERNAL TABLE: {:?}",
                    self.parser.peek_token()
                )),
            }
        } else {
            Ok(DFASTNode::ANSI(self.parser.parse_prefix()?))
        }
    }

    pub fn parse_infix(
        &mut self,
        _expr: DFASTNode,
        _precedence: u8,
    ) -> Result<Option<DFASTNode>, ParserError> {
        unimplemented!()
    }
}
