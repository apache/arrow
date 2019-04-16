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

use crate::parquet_field::ThirdPartyTypes::ChronoNaiveDateTime;

#[derive(Debug, PartialEq)]
pub struct Field {
    ident: syn::Ident,
    ty: Type,
    is_a_byte_buf: bool,
    third_party_type: Option<ThirdPartyType>
}

mod FieldHelpers {
    fn option_into_vals(
        id: &syn::Ident,
        is_a_byte_buf: bool,
        is_a_timestamp: bool,
        copy_to_vec: bool,
    ) -> proc_macro2::TokenStream {
        let binding = if copy_to_vec {
            quote! { let Some(inner) = x.#id }
        } else {
            quote! { let Some(ref inner) = x.#id }
        };

        let some = if is_a_byte_buf {
            quote! { Some((&inner[..]).into())}
        } else if is_a_timestamp {
            quote! { Some(inner.timestamp_millis()) }
        } else {
            quote! { Some(inner) }
        };

        quote! {
                let vals: Vec<_> = records.iter().filter_map(|x| {
                    if #binding {
                        #some
                    } else {
                        None
                    }
                }).collect();
            }
    }

    fn copied_direct_vals(
        id: &syn::Ident,
        is_a_byte_buf: bool,
        is_a_timestamp: bool,
    ) -> proc_macro2::TokenStream {
        let access = if is_a_byte_buf {
            quote! { (&x.#id[..]).into() }
        } else if is_a_timestamp {
            quote!( x.#id.timestamp_millis() )
        } else {
            quote! { x.#id }
        };

        quote! {
                let vals: Vec<_> = records.iter().map(|x| #access).collect();
            }
    }

    fn optional_definition_levels(id: &syn::Ident) -> proc_macro2::TokenStream {
        quote! {
                let definition_levels: Vec<i16> = self
                  .iter()
                  .map(|x| if x.#id.is_some() { 1 } else { 0 })
                  .collect();
            }
    }
}

/// Use third party libraries, detected
/// at compile time. These libraries will
/// be written to parquet as their preferred
/// physical type.
///
///   ChronoNaiveDateTime is written as i64
///   ChronoNaiveDate is written as i32
enum ThirdPartyType {
    ChronoNaiveDateTime,
    ChronoNaiveDate,
}

impl Field {
    pub fn from(f: &syn::Field) -> Self {
        let ty = Type::from(f);
        let is_a_byte_buf = ty.physical_type() == parquet::basic::Type::BYTE_ARRAY;

        let third_party_type = match &ty.last_part()[..] {
            "NaiveDateTime" => Some(ThirdPartyType::ChronoNaiveDateTime),
            "NaiveDate" => Some(ThirdPartyType::ChronoNaiveDate),
            _ => None
        };

        Field {
            ident: f
                .ident
                .clone()
                .expect("we only support structs with named fields"),
            ty,
            is_a_byte_buf,
            third_party_type,
        }
    }

    /// Takes the parsed field of the struct and emits a valid
    /// column writer snippet.
    ///
    /// Can only generate writers for basic structs, for example:
    ///
    /// struct Record {
    ///   a_bool: bool,
    ///   maybe_a_bool: Option<bool>
    /// }
    ///
    /// but not
    ///
    /// struct UnsupportedNestedRecord {
    ///   a_property: bool,
    ///   nested_record: Record
    /// }
    ///
    /// because this parsing logic is not sophisticated enough for definition
    /// levels beyond 2.
    pub fn writer_snippet(&self) -> proc_macro2::TokenStream {
        let ident = &self.ident;
        let column_writer = self.ty.column_writer();
        let copy_to_vec = match self.ty.physical_type() {
            parquet::basic::Type::BYTE_ARRAY
            | parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => false,
            _ => true,
        };

        let vals_builder = match &self.ty {
            Type::TypePath(_) => copied_direct_vals(ident, is_a_byte_buf, is_a_timestamp),
            Type::Option(ref first_type) => match **first_type {
                Type::TypePath(_) => option_into_vals(ident, is_a_byte_buf, is_a_timestamp,copy_to_vec),
                Type::Reference(_, ref second_type) => match **second_type {
                    Type::TypePath(_) => {
                        option_into_vals(ident, is_a_byte_buf, is_a_timestamp, copy_to_vec)
                    }
                    _ => unimplemented!("sorry charlie"),
                },
                ref f @ _ => unimplemented!("whoa: {:#?}", f),
            },
            Type::Reference(_, ref first_type) => match **first_type {
                Type::TypePath(_) => copied_direct_vals(ident, is_a_byte_buf, is_a_timestamp),
                Type::Option(ref second_type) => match **second_type {
                    Type::TypePath(_) => {
                        option_into_vals(ident, is_a_byte_buf, is_a_timestamp, copy_to_vec)
                    }
                    Type::Reference(_, ref second_type) => match **second_type {
                        Type::TypePath(_) => {
                            option_into_vals(ident, is_a_byte_buf, is_a_timestamp, copy_to_vec)
                        }
                        _ => unimplemented!("sorry charlie"),
                    },
                    ref f @ _ => unimplemented!("whoa: {:#?}", f),
                },
                ref f @ _ => unimplemented!("whoa: {:#?}", f),
            },
            f @ _ => unimplemented!("don't support {:#?}", f),
        };

        let definition_levels = match &self.ty {
            Type::TypePath(_) => None,
            Type::Option(ref first_type) => match **first_type {
                Type::TypePath(_) => Some(optional_definition_levels(ident)),
                Type::Option(_) => unimplemented!("nested options? that's weird"),
                Type::Reference(_, ref second_type)
                | Type::Vec(ref second_type)
                | Type::Array(ref second_type) => match **second_type {
                    Type::TypePath(_) => Some(optional_definition_levels(ident)),
                    _ => unimplemented!("a little too much nesting. bailing out."),
                },
            },
            Type::Reference(_, ref first_type)
            | Type::Vec(ref first_type)
            | Type::Array(ref first_type) => match **first_type {
                Type::TypePath(_) => None,
                Type::Reference(_, ref second_type)
                | Type::Vec(ref second_type)
                | Type::Array(ref second_type)
                | Type::Option(ref second_type) => match **second_type {
                    Type::TypePath(_) => Some(optional_definition_levels(ident)),
                    Type::Reference(_, ref third_type) => match **third_type {
                        Type::TypePath(_) => Some(optional_definition_levels(ident)),
                        _ => unimplemented!(
                            "we don't do some more complex definition levels... yet!"
                        ),
                    },
                    _ => unimplemented!(
                        "we don't do more complex definition levels... yet!"
                    ),
                },
            },
        };

        let write_batch_expr = if definition_levels.is_some() {
            quote! {
                if let #column_writer(ref mut typed) = column_writer {
                    typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
                } else {
                    panic!("schema and struct disagree on type for {}", stringify!{#ident})
                }
            }
        } else {
            quote! {
                if let #column_writer(ref mut typed) = column_writer {
                    typed.write_batch(&vals[..], None, None).unwrap();
                } else {
                    panic!("schema and struct disagree on type for {}", stringify!{#ident})
                }
            }
        };

        quote! {
            {
                #definition_levels

                #vals_builder

                #write_batch_expr
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Type {
    Array(Box<Type>),
    Option(Box<Type>),
    Vec(Box<Type>),
    TypePath(syn::Type),
    Reference(Option<syn::Lifetime>, Box<Type>),
}

impl Type {
    pub fn column_writer(&self) -> syn::TypePath {
        use parquet::basic::Type as BasicType;

        let path = match self.physical_type() {
            BasicType::BOOLEAN => {
                syn::parse_str("parquet::column::writer::ColumnWriter::BoolColumnWriter")
            }
            BasicType::INT32 => {
                syn::parse_str("parquet::column::writer::ColumnWriter::Int32ColumnWriter")
            }
            BasicType::INT64 => {
                syn::parse_str("parquet::column::writer::ColumnWriter::Int64ColumnWriter")
            }
            BasicType::INT96 => {
                syn::parse_str("parquet::column::writer::ColumnWriter::Int96ColumnWriter")
            }
            BasicType::FLOAT => {
                syn::parse_str("parquet::column::writer::ColumnWriter::FloatColumnWriter")
            }
            BasicType::DOUBLE => syn::parse_str(
                "parquet::column::writer::ColumnWriter::DoubleColumnWriter",
            ),
            BasicType::BYTE_ARRAY => syn::parse_str(
                "parquet::column::writer::ColumnWriter::ByteArrayColumnWriter",
            ),
            BasicType::FIXED_LEN_BYTE_ARRAY => syn::parse_str(
                "parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter",
            ),
        };
        path.unwrap()
    }

    fn inner_type(&self) -> &syn::Type {
        let leaf_type = self.leaf_type();

        match leaf_type {
            Type::TypePath(ref type_) => type_,
            Type::Option(ref first_type)
            | Type::Vec(ref first_type)
            | Type::Array(ref first_type)
            | Type::Reference(_, ref first_type) => match **first_type {
                Type::TypePath(ref type_) => type_,
                _ => unimplemented!("leaf_type() should only return shallow types"),
            },
        }
    }

    /// Helper to simplify a nested field definition to its leaf type
    ///
    /// Ex:
    ///   Option<&String> => Type::TypePath(String)
    ///   &Option<i32> => Type::TypePath(i32)
    ///   Vec<Vec<u8>> => Type::Vec(u8)
    ///
    /// Useful in determining the physical type of a field and the
    /// definition levels.
    ///
    pub fn leaf_type(&self) -> &Type {
        match &self {
            Type::TypePath(_) => &self,
            Type::Option(ref first_type)
            | Type::Vec(ref first_type)
            | Type::Array(ref first_type)
            | Type::Reference(_, ref first_type) => match **first_type {
                Type::TypePath(_) => &self,
                Type::Option(ref second_type)
                | Type::Vec(ref second_type)
                | Type::Array(ref second_type)
                | Type::Reference(_, ref second_type) => match **second_type {
                    Type::TypePath(_) => first_type,
                    Type::Option(ref third_type)
                    | Type::Vec(ref third_type)
                    | Type::Array(ref third_type)
                    | Type::Reference(_, ref third_type) => match **third_type {
                        Type::TypePath(_) => second_type,
                        Type::Option(ref fourth_type)
                        | Type::Vec(ref fourth_type)
                        | Type::Array(ref fourth_type)
                        | Type::Reference(_, ref fourth_type) => match **fourth_type {
                            Type::TypePath(_) => third_type,
                            _ => unimplemented!("sorry, I don't descend this far"),
                        },
                    },
                },
            },
        }
    }

    ///
    /// Helper to normalize a type path by extracting the
    /// most identifiable part
    ///
    /// Ex:
    ///   std::string::String => String
    ///   Vec<u8> => Vec<u8>
    ///   chrono::NaiveDateTime => NaiveDateTime
    ///
    /// Does run the risk of mis-identifying a type if import
    /// rename is in play. Please note procedural macros always
    /// run before type resolution so this is a risk the user
    /// takes on when renaming imports.
    ///
    pub fn last_part(&self) -> String {
        let inner_type = self.inner_type();
        let inner_type_str = (quote! { #inner_type }).to_string();

        inner_type_str.split("::").last().unwrap().trim().to_string()
    }

    ///
    /// Converts rust types to parquet physical types.
    ///
    /// Ex:
    ///   [u8; 10] => FIXED_LEN_BYTE_ARRAY
    ///   Vec<u8>  => BYTE_ARRAY
    ///   String => BYTE_ARRAY
    ///   i32 => INT32
    pub fn physical_type(&self) -> parquet::basic::Type {
        use parquet::basic::Type as BasicType;

        let last_part = self.last_part();
        let leaf_type = self.leaf_type();

        match leaf_type {
            Type::Array(ref first_type) => {
                if let Type::TypePath(_) = **first_type {
                    if last_part == "u8" {
                        return BasicType::FIXED_LEN_BYTE_ARRAY;
                    }
                }
            }
            Type::Vec(ref first_type) => {
                if let Type::TypePath(_) = **first_type {
                    if last_part == "u8" {
                        return BasicType::BYTE_ARRAY;
                    }
                }
            }
            _ => (),
        }

        match last_part.trim() {
            "bool" => BasicType::BOOLEAN,
            "u8" | "u16" | "u32" => BasicType::INT32,
            "i8" | "i16" | "i32" => BasicType::INT32,
            "u64" | "i64" | "usize" | "NaiveDateTime" => BasicType::INT64,
            "f32" => BasicType::FLOAT,
            "f64" => BasicType::DOUBLE,
            "String" | "str" => BasicType::BYTE_ARRAY,
            f @ _ => unimplemented!("sorry, don't handle {} yet!", f),
        }
    }

    ///
    /// Convert a parsed rust field AST in to a more easy to manipulate
    /// parquet_derive::Field
    ///
    pub fn from_type_path(f: &syn::Field, p: &syn::TypePath) -> Self {
        let segments_iter = p.path.segments.iter();
        let segments: Vec<syn::PathSegment> = segments_iter.map(|x| x.clone()).collect();
        let last_segment = segments.last().unwrap();

        let is_vec =
            last_segment.ident == syn::Ident::new("Vec", proc_macro2::Span::call_site());
        let is_option = last_segment.ident
            == syn::Ident::new("Option", proc_macro2::Span::call_site());

        if is_vec || is_option {
            let generic_type = match &last_segment.arguments {
                syn::PathArguments::AngleBracketed(angle_args) => {
                    let mut gen_args_iter = angle_args.args.iter();
                    let first_arg = gen_args_iter.next().unwrap();
                    assert!(gen_args_iter.next().is_none());

                    match first_arg {
                        syn::GenericArgument::Type(ref typath) => typath.clone(),
                        other @ _ => unimplemented!("don't know {:#?}", other),
                    }
                }
                other @ _ => unimplemented!("don't know: {:#?}", other),
            };

            if is_vec {
                Type::Vec(Box::new(Type::from_type(f, &generic_type)))
            } else {
                Type::Option(Box::new(Type::from_type(f, &generic_type)))
            }
        } else {
            Type::TypePath(syn::Type::Path(p.clone()))
        }
    }

    pub fn from_type_reference(f: &syn::Field, tr: &syn::TypeReference) -> Self {
        let lifetime = tr.lifetime.clone();
        let inner_type = Type::from_type(f, tr.elem.as_ref());
        Type::Reference(lifetime, Box::new(inner_type))
    }

    pub fn from_type_array(f: &syn::Field, ta: &syn::TypeArray) -> Self {
        let inner_type = Type::from_type(f, ta.elem.as_ref());
        Type::Array(Box::new(inner_type))
    }

    pub fn from_type(f: &syn::Field, ty: &syn::Type) -> Self {
        match ty {
            syn::Type::Path(ref p) => Type::from_type_path(f, p),
            syn::Type::Reference(ref tr) => Type::from_type_reference(f, tr),
            syn::Type::Array(ref ta) => Type::from_type_array(f, ta),
            other @ _ => unimplemented!(
                "we can't derive for {:?}. it is an unsupported type\n{:#?}",
                f.ident.as_ref().unwrap(),
                other
            ),
        }
    }

    pub fn from(f: &syn::Field) -> Self {
        Type::from_type(f, &f.ty)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use syn::{self, Data, DataStruct, DeriveInput};

    fn extract_fields(input: proc_macro2::TokenStream) -> Vec<syn::Field> {
        let input: DeriveInput = syn::parse2(input).unwrap();

        let fields = match input.data {
            Data::Struct(DataStruct { fields, .. }) => fields,
            _ => panic!("input must be a struct"),
        };

        fields.iter().map(|x| x.to_owned()).collect()
    }

    #[test]
    fn test_generating_a_simple_writer_snippet() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ABoringStruct {
            counter: usize,
          }
        };

        let fields = extract_fields(snippet);
        let counter = Field::from(&fields[0]);

        let snippet = counter.writer_snippet().to_string();
        assert_eq!(snippet,
                   (quote!{
                        {
                            let vals : Vec < _ > = records . iter ( ) . map ( | x | x . counter ) . collect ( );

                            if let parquet::column::writer::ColumnWriter::Int64ColumnWriter ( ref mut typed ) = column_writer {
                                typed . write_batch ( & vals [ .. ] , None , None ) . unwrap ( );
                            }  else {
                                panic!("schema and struct disagree on type for {}" , stringify!{ counter } )
                            }
                        }
                   }).to_string()
        )
    }

    #[test]
    fn test_optional_to_writer_snippet() {
        let struct_def: proc_macro2::TokenStream = quote! {
          struct StringBorrower<'a> {
            optional_str: Option<&'a str>,
            optional_string: &Option<String>,
            optional_dumb_int: &Option<&i32>,
          }
        };

        let fields = extract_fields(struct_def);

        let optional = Field::from(&fields[0]);
        let snippet = optional.writer_snippet();
        assert_eq!(snippet.to_string(),
          (quote! {
          {
                let definition_levels : Vec < i16 > = self . iter ( ) . map ( | x | if x . optional_str . is_some ( ) { 1 } else { 0 } ) . collect ( ) ;

                let vals: Vec <_> = records.iter().filter_map( |x| {
                    if let Some ( ref inner ) = x . optional_str {
                        Some ( (&inner[..]).into() )
                    } else {
                        None
                    }
                }).collect();

                if let parquet::column::writer::ColumnWriter::ByteArrayColumnWriter ( ref mut typed ) = column_writer {
                    typed . write_batch ( & vals [ .. ] , Some(&definition_levels[..]) , None ) . unwrap ( ) ;
                } else {
                    panic!("schema and struct disagree on type for {}" , stringify ! { optional_str } )
                }
           }
            }
          ).to_string());

        let optional = Field::from(&fields[1]);
        let snippet = optional.writer_snippet();
        assert_eq!(snippet.to_string(),
                   (quote!{
                   {
                        let definition_levels : Vec < i16 > = self . iter ( ) . map ( | x | if x . optional_string . is_some ( ) { 1 } else { 0 } ) . collect ( ) ;

                        let vals: Vec <_> = records.iter().filter_map( |x| {
                            if let Some ( ref inner ) = x . optional_string {
                                Some ( (&inner[..]).into() )
                            } else {
                                None
                            }
                        }).collect();

                        if let parquet::column::writer::ColumnWriter::ByteArrayColumnWriter ( ref mut typed ) = column_writer {
                            typed . write_batch ( & vals [ .. ] , Some(&definition_levels[..]) , None ) . unwrap ( ) ;
                        } else {
                            panic!("schema and struct disagree on type for {}" , stringify ! { optional_string } )
                        }
                    }
        }).to_string());

        let optional = Field::from(&fields[2]);
        let snippet = optional.writer_snippet();
        assert_eq!(snippet.to_string(),
                   (quote!{
                    {
                        let definition_levels : Vec < i16 > = self . iter ( ) . map ( | x | if x . optional_dumb_int . is_some ( ) { 1 } else { 0 } ) . collect ( ) ;

                        let vals: Vec <_> = records.iter().filter_map( |x| {
                            if let Some ( inner ) = x . optional_dumb_int {
                                Some ( inner )
                            } else {
                                None
                            }
                        }).collect();

                        if let parquet::column::writer::ColumnWriter::Int32ColumnWriter ( ref mut typed ) = column_writer {
                            typed . write_batch ( & vals [ .. ] , Some(&definition_levels[..]) , None ) . unwrap ( ) ;
                        }  else {
                            panic!("schema and struct disagree on type for {}" , stringify ! { optional_dumb_int } )
                        }
                    }
        }).to_string());
    }

    #[test]
    fn test_converting_to_column_writer_type() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ABasicStruct {
            yes_no: bool,
            name: String,
          }
        };

        let fields = extract_fields(snippet);
        let processed: Vec<_> = fields.iter().map(|x| Field::from(x)).collect();

        let column_writers: Vec<_> =
            processed.iter().map(|x| x.ty.column_writer()).collect();

        assert_eq!(
            column_writers,
            vec![
                syn::parse_str("parquet::column::writer::ColumnWriter::BoolColumnWriter")
                    .unwrap(),
                syn::parse_str(
                    "parquet::column::writer::ColumnWriter::ByteArrayColumnWriter"
                )
                .unwrap()
            ]
        );
    }

    #[test]
    fn convert_basic_struct() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ABasicStruct {
            yes_no: bool,
            name: String,
          }
        };

        let fields = extract_fields(snippet);
        let processed: Vec<_> = fields.iter().map(|x| Field::from(x)).collect();
        assert_eq!(processed.len(), 2);

        assert_eq!(
            processed,
            vec![
                Field {
                    ident: syn::Ident::new("yes_no", proc_macro2::Span::call_site()),
                    ty: Type::TypePath(syn::parse_str("bool").unwrap())
                },
                Field {
                    ident: syn::Ident::new("name", proc_macro2::Span::call_site()),
                    ty: Type::TypePath(syn::parse_str("String").unwrap())
                }
            ]
        )
    }

    #[test]
    fn test_get_inner_type() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct LotsOfInnerTypes {
            a_vec: Vec<u8>,
            a_option: std::option::Option<bool>,
            a_silly_string: std::string::String,
            a_complicated_thing: std::option::Option<std::result::Result<(),()>>,
          }
        };

        let fields = extract_fields(snippet);
        let converted_fields: Vec<_> = fields.iter().map(|x| Type::from(x)).collect();
        let inner_types: Vec<_> =
            converted_fields.iter().map(|x| x.inner_type()).collect();
        let inner_types_strs: Vec<_> = inner_types
            .iter()
            .map(|x| (quote! { #x }).to_string())
            .collect();

        assert_eq!(
            inner_types_strs,
            vec![
                "u8",
                "bool",
                "std :: string :: String",
                "std :: result :: Result < ( ) , ( ) >"
            ]
        )
    }

    #[test]
    fn test_physical_type() {
        use parquet::basic::Type as BasicType;
        let snippet: proc_macro2::TokenStream = quote! {
          struct LotsOfInnerTypes {
            a_buf: Vec<u8>,
            a_number: i32,
            a_verbose_option: std::option::Option<bool>,
            a_silly_string: std::string::String,
            a_fix_byte_buf: [u8; 10],
            a_complex_option: Option<&Vec<u8>>,
            a_complex_vec: &Vec<&Option<u8>>,
          }
        };

        let fields = extract_fields(snippet);
        let converted_fields: Vec<_> = fields.iter().map(|x| Type::from(x)).collect();
        let physical_types: Vec<_> =
            converted_fields.iter().map(|x| x.physical_type()).collect();

        assert_eq!(
            physical_types,
            vec![
                BasicType::BYTE_ARRAY,
                BasicType::INT32,
                BasicType::BOOLEAN,
                BasicType::BYTE_ARRAY,
                BasicType::FIXED_LEN_BYTE_ARRAY,
                BasicType::BYTE_ARRAY,
                BasicType::INT32
            ]
        )
    }

    #[test]
    fn convert_comprehensive_owned_struct() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct VecHolder {
            a_vec: Vec<u8>,
            a_option: std::option::Option<bool>,
            a_silly_string: std::string::String,
            a_complicated_thing: std::option::Option<std::result::Result<(),()>>,
          }
        };

        let fields = extract_fields(snippet);
        let converted_fields: Vec<_> = fields.iter().map(|x| Type::from(x)).collect();

        assert_eq!(
            converted_fields,
            vec![
                Type::Vec(Box::new(Type::TypePath(
                    syn::parse_str::<syn::Type>("u8").unwrap()
                ))),
                Type::Option(Box::new(Type::TypePath(
                    syn::parse_str::<syn::Type>("bool").unwrap()
                ))),
                Type::TypePath(
                    syn::parse_str::<syn::Type>("std::string::String").unwrap()
                ),
                Type::Option(Box::new(Type::TypePath(
                    syn::parse_str::<syn::Type>("std::result::Result<(),()>").unwrap()
                ))),
            ]
        );
    }

    #[test]
    fn convert_borrowed_struct() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct Borrower<'a> {
            a_str: &'a str,
            a_borrowed_option: &'a Option<bool>,
            so_many_borrows: &'a Option<&'a str>,
          }
        };

        let fields = extract_fields(snippet);
        let types: Vec<_> = fields.iter().map(|x| Type::from(x)).collect();

        assert_eq!(
            types,
            vec![
                Type::Reference(
                    Some(syn::Lifetime::new("'a", proc_macro2::Span::call_site())),
                    Box::new(Type::TypePath(syn::parse_str("str").unwrap()))
                ),
                Type::Reference(
                    Some(syn::Lifetime::new("'a", proc_macro2::Span::call_site())),
                    Box::new(Type::Option(Box::new(Type::TypePath(
                        syn::parse_str("bool").unwrap()
                    ))))
                ),
                Type::Reference(
                    Some(syn::Lifetime::new("'a", proc_macro2::Span::call_site())),
                    Box::new(Type::Option(Box::new(Type::Reference(
                        Some(syn::Lifetime::new("'a", proc_macro2::Span::call_site())),
                        Box::new(Type::TypePath(syn::parse_str("str").unwrap()))
                    ))))
                ),
            ]
        );
    }

    #[test]
    fn fail_on_tuple() {
        return;
        let snippet: proc_macro2::TokenStream = quote! {
          struct TupleHolder {
            unsupported_for_now: (bool, String)
          }
        };

        let fields = extract_fields(snippet);
        let res = std::panic::catch_unwind(move || {
            let _ = fields.iter().map(|x| Type::from(x)).collect::<Vec<_>>();
        });
        assert!(res.is_err());
    }


    #[test]
    #[cfg(feature = "chrono")]
    fn chrono_timestamp_millis() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ATimestampStruct {
            henceforth: chrono::NaiveDateTime,
            maybe_happened: Option<&chrono::NaiveDateTime>,
          }
        };

        let fields = extract_fields(snippet);
        let when = Field::from(&fields[0]);
        assert_eq!(when.writer_snippet().to_string(),(quote!{
            {
                let vals : Vec<_> = records.iter().map(|x| x.henceforth.timestamp_millis() ).collect();
                if let parquet::column::writer::ColumnWriter::Int64ColumnWriter(ref mut typed) = column_writer {
                    typed.write_batch(&vals[..], None, None).unwrap();
                } else {
                    panic!("schema and struct disagree on type for {}" , stringify!{ henceforth })
                }
            }
        }).to_string());

        let maybe_happened = Field::from(&fields[1]);
        assert_eq!(maybe_happened.writer_snippet().to_string(),(quote!{
            {
                let definition_levels : Vec<i16> = self.iter().map(|x| if x.maybe_happened.is_some() { 1 } else { 0 }).collect();
                let vals : Vec<_> = records.iter().filter_map(|x| {
                    if let Some(inner) = x.maybe_happened {
                        Some( inner.timestamp_millis() )
                    } else {
                        None
                    }
                }).collect();

                if let parquet::column::writer::ColumnWriter::Int64ColumnWriter(ref mut typed) = column_writer {
                    typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
                } else {
                    panic!("schema and struct disagree on type for {}" , stringify!{ maybe_happened })
                }
            }
        }).to_string());
    }
}
