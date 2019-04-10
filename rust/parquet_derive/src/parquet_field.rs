#[derive(Debug, PartialEq)]
pub struct Field {
    ident: syn::Ident,
    ty: Type,
}

impl Field {
    pub fn from(f: &syn::Field) -> Self {
        Field {
            ident: f
                .ident
                .clone()
                .expect("we only support structs with named fields"),
            ty: Type::from(f),
        }
    }

    //  fn column_writer_variant(&self) -> Path {
    //    let ftype_string = self.field_type.to_string();
    //    let ftype_string = if &ftype_string[..] == "Option" {
    //      let fga: &FieldInfoGenericArg = self
    //        .field_generic_arguments
    //        .get(0)
    //        .expect("must have at least 1 generic argument");
    //      let field_type = &fga.field_type;
    //
    //      let field_type_token_stream = quote!{ #field_type };
    //      field_type_token_stream.to_string()
    //    } else {
    //      ftype_string
    //    };
    //
    ////    if &ftype_string[..] != "Option" {
    ////      panic!("blarp: {}", ftype_string);
    ////    }
    //
    //    Self::ident_to_column_writer_variant(&ftype_string[..]).unwrap_or_else(|f| {
    //      panic!("no column writer variant for {}/{}. err: `{}` ", ftype_string,
    // self.field_type, f)    })
    //  }

    fn ident_to_column_writer_variant(
        id: &str,
    ) -> Result<syn::Path, Box<dyn std::error::Error>> {
        let column_writer_variant = match id {
            "bool" => quote! { parquet::column::writer::ColumnWriter::BoolColumnWriter },
            "str" | "String" | "Vec" => {
                quote! { parquet::column::writer::ColumnWriter::ByteArrayColumnWriter }
            }
            "i32" => {
                quote! { parquet::column::writer::ColumnWriter::Int32ColumnWriter }
            }
            "u32" => {
                quote! { parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter }
            }
            "i64" | "u64" => {
                quote! { parquet::column::writer::ColumnWriter::Int64ColumnWriter }
            }
            "f32" => quote! { parquet::column::writer::ColumnWriter::FloatColumnWriter },
            "f64" => quote! { parquet::column::writer::ColumnWriter::DoubleColumnWriter },
            o => {
                //        unimplemented!("don't know column writer variant for {}", o)
                use std::io::{Error as IOError, ErrorKind as IOErrorKind};
                //        unimplemented!()
                return Err(Box::new(IOError::new(
                    IOErrorKind::Other,
                    format!("don't know {}", o),
                )));
            }
        };

        syn::parse2(column_writer_variant).map_err(|x| panic!("x: {:?}", x))
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
            BasicType::BOOLEAN => syn::parse_str("parquet::column::writer::ColumnWriter::BoolColumnWriter"),
            BasicType::INT32 => syn::parse_str("parquet::column::writer::ColumnWriter::Int32ColumnWriter"),
            BasicType::INT64 => syn::parse_str("parquet::column::writer::ColumnWriter::Int64ColumnWriter"),
            BasicType::INT96 => syn::parse_str("parquet::column::writer::ColumnWriter::Int96ColumnWriter"),
            BasicType::FLOAT => syn::parse_str("parquet::column::writer::ColumnWriter::FloatColumnWriter"),
            BasicType::DOUBLE => syn::parse_str("parquet::column::writer::ColumnWriter::DoubleColumnWriter"),
            BasicType::BYTE_ARRAY => syn::parse_str("parquet::column::writer::ColumnWriter::ByteArrayColumnWriter"),
            BasicType::FIXED_LEN_BYTE_ARRAY => syn::parse_str("parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter"),
        };
        path.unwrap()
    }

    fn inner_type(&self) -> &syn::Type {
        match self.leaf_type() {
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
                        | Type::Reference(_, ref fourth_type) => {
                            match **fourth_type {
                                Type::TypePath(_) => third_type,
                                _ => unimplemented!("sorry, I don't descend this far")
                            }
                        },
                    },
                    _ => unimplemented!("sorry thirdsies!"),
                },
                _ => unimplemented!("sorry secondsies!"),
            },
            f @ _ => unimplemented!("sorry again! {:#?}", f),
        }
    }

    pub fn physical_type(&self) -> parquet::basic::Type {
        use parquet::basic::Type as BasicType;

        let inner_type = self.inner_type();
        let inner_type_str = (quote! { #inner_type }).to_string();
        let last_part = inner_type_str.split("::").last().unwrap().trim();

        match self.leaf_type() {
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
            "u64" | "i64" => BasicType::INT64,
            "f32" => BasicType::FLOAT,
            "f64" => BasicType::DOUBLE,
            "String" | "str" => BasicType::BYTE_ARRAY,
            f @ _ => unimplemented!("sorry, don't handle {} yet!", f),
        }
    }

    pub fn from_type_path(f: &syn::Field, p: &syn::TypePath) -> Self {
        let mut segments_iter = p.path.segments.iter();
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
    fn test_converting_to_column_writer_type() {
        let snippet: proc_macro2::TokenStream = quote! {
          struct ABasicStruct {
            yes_no: bool,
            name: String,
          }
        };

        let fields = extract_fields(snippet);
        let processed : Vec<_> = fields.iter().map(|x| Field::from(x)).collect();

        let column_writers: Vec<_> = processed.iter().map(|x| x.ty.column_writer()).collect();

        assert_eq!(column_writers, vec![
            syn::parse_str("parquet::column::writer::ColumnWriter::BoolColumnWriter").unwrap(),
            syn::parse_str("parquet::column::writer::ColumnWriter::ByteArrayColumnWriter").unwrap()
        ]);
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
}
