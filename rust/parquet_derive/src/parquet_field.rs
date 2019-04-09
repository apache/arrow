#[derive(Debug,PartialEq)]
pub struct Field {
  ident: syn::Ident,
  ty: Type,
}

impl Field {
  pub fn from(f: &syn::Field) -> Self {
    Field {
      ident: f.ident.clone().expect("we only support structs with named fields"),
      ty: Type::from(f)
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
//      panic!("no column writer variant for {}/{}. err: `{}` ", ftype_string, self.field_type, f)
//    })
//  }

  fn ident_to_column_writer_variant(id: &str) -> Result<syn::Path, Box<dyn std::error::Error>> {
    let column_writer_variant = match id {
      "bool" => quote! { parquet::column::writer::ColumnWriter::BoolColumnWriter },
      "str" | "String" | "Vec" => {
        quote! { parquet::column::writer::ColumnWriter::ByteArrayColumnWriter }
      },
      "i32" => {
        quote! { parquet::column::writer::ColumnWriter::Int32ColumnWriter }
      },
      "u32" => {
        quote! { parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter }
      },
      "i64" | "u64" => {
        quote! { parquet::column::writer::ColumnWriter::Int64ColumnWriter }
      },
      "f32" => quote! { parquet::column::writer::ColumnWriter::FloatColumnWriter },
      "f64" => quote! { parquet::column::writer::ColumnWriter::DoubleColumnWriter },
      o => {
        //        unimplemented!("don't know column writer variant for {}", o)
        use std::io::{Error as IOError, ErrorKind as IOErrorKind};
//        unimplemented!()
        return Err(Box::new(IOError::new(IOErrorKind::Other, format!("don't know {}", o))))
      },
    };

//    unimplemented!()
    syn::parse2(column_writer_variant).map_err(|x| panic!("x: {:?}", x))
  }
}

#[derive(Debug,PartialEq)]
pub enum Type {
  Option(Box<Type>),
  Vec(Box<Type>),
  TypePath(syn::Type),
  Reference(Option<syn::Lifetime>, Box<Type>),
}

impl Type {
  pub fn physical_type(&self) -> parquet::basic::Type {
    let inner_type : &syn::Type = match &self {
      Type::Option(generic) | Type::Vec(generic) => {
        match **generic {
          Type::TypePath(ref type_) => type_,
          _ => unimplemented!("nesting generics is not supported")
        }
      },
      Type::TypePath(ref type_) => type_,
      Type::Reference(_, ref first_type) => {
        match **first_type {
          Type::TypePath(ref type_) => type_,
          Type::Option(ref second_type) | Type::Vec(ref second_type) => {
            match **second_type {
              Type::TypePath(ref type_) => type_,
              Type::Reference(_, ref third_type) => {
                match **third_type {
                  Type::TypePath(ref type_) => type_,
                  Type::Vec(ref fourth_type) | Type::Option(ref fourth_type) => {
                    match **fourth_type {
                      Type::TypePath(ref type_) => type_,
                      _ => unimplemented!("only two levels of generic nesting supported")
                    }
                  },
                  _ => unimplemented!("references can ")
                }
              }
              Type::Option(_) | Type::Vec(_) => unimplemented!("options cannot hold Option or Vec")
            }
          },
          Type::Reference(_,_) => unimplemented!("double references are not supported")
        }
      }
    };

    unimplemented!("hi mom")
//    BOOLEAN,
//    INT32,
//    INT64,
//    INT96,
//    FLOAT,
//    DOUBLE,
//    BYTE_ARRAY,
//    FIXED_LEN_BYTE_ARRAY,
  }

  pub fn from_type_path(f: &syn::Field, p: &syn::TypePath) -> Self {
    let mut segments_iter = p.path.segments.iter();
    let segments: Vec<syn::PathSegment> = segments_iter.map(|x| x.clone()).collect();
    let last_segment = segments.last().unwrap();

    let is_vec = last_segment.ident == syn::Ident::new("Vec", proc_macro2::Span::call_site());
    let is_option = last_segment.ident == syn::Ident::new("Option", proc_macro2::Span::call_site());

    if is_vec || is_option {
      let generic_type = match &last_segment.arguments {
        syn::PathArguments::AngleBracketed(angle_args) => {
          let mut gen_args_iter = angle_args.args.iter();
          let first_arg = gen_args_iter.next().unwrap();
          assert!(gen_args_iter.next().is_none());

          match first_arg {
            syn::GenericArgument::Type(ref typath) => typath.clone(),
            other @ _ => unimplemented!("don't know {:#?}", other)
          }
        },
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
    let inner_type = Type::from_type(f,tr.elem.as_ref());
    Type::Reference(lifetime, Box::new(inner_type))
  }

  pub fn from_type(f: &syn::Field, ty: &syn::Type) -> Self {
    match ty {
      syn::Type::Path(ref p) => {
        Type::from_type_path(f,p)
      },
      syn::Type::Reference(ref tr) => {
        Type::from_type_reference(f,tr)
      },
      other @ _ => {
        unimplemented!("we can't derive for {:?}. it is an unsupported type\n{:#?}", f.ident.as_ref().unwrap(), other)
      }
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
  fn convert_basic_struct() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct ABasicStruct {
        yes_no: bool,
        name: String,
      }
    };

    let fields = extract_fields(snippet);
    let processed : Vec<_> = fields.iter().map(|x| Field::from(x)).collect();
    assert_eq!(processed.len(), 2);

    assert_eq!(processed, vec![
      Field {
        ident: syn::Ident::new("yes_no", proc_macro2::Span::call_site()),
        ty: Type::TypePath(syn::parse_str("bool").unwrap())
      },
      Field {
        ident: syn::Ident::new("name", proc_macro2::Span::call_site()),
        ty: Type::TypePath(syn::parse_str("String").unwrap())
      }
    ])
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
    assert_eq!(fields.len(), 4);

    let generic_type = syn::parse_str::<syn::Type>("u8").unwrap();
    assert_eq!(Type::from(&fields[0]), Type::Vec(Box::new(Type::TypePath(generic_type))));

    let generic_type = syn::parse_str::<syn::Type>("bool").unwrap();
    assert_eq!(Type::from(&fields[1]), Type::Option(Box::new(Type::TypePath(generic_type))));

    let string_path = syn::parse_str::<syn::Type>("std::string::String").unwrap();
    assert_eq!(Type::from(&fields[2]), Type::TypePath(string_path));

    let string_path = syn::parse_str::<syn::Type>("std::result::Result<(),()>").unwrap();
    assert_eq!(Type::from(&fields[3]), Type::Option(Box::new(Type::TypePath(string_path))));
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
    let types : Vec<_> = fields.iter().map(|x| Type::from(x)).collect();

    assert_eq!(types, vec![
      Type::Reference(
        Some(syn::Lifetime::new("'a",proc_macro2::Span::call_site())),
        Box::new(Type::TypePath(syn::parse_str("str").unwrap()))
      ),
      Type::Reference(
        Some(syn::Lifetime::new("'a",proc_macro2::Span::call_site())),
        Box::new(Type::Option(
            Box::new(Type::TypePath(syn::parse_str("bool").unwrap()))
        ))
      ),
      Type::Reference(
        Some(syn::Lifetime::new("'a",proc_macro2::Span::call_site())),
        Box::new(Type::Option(Box::new(Type::Reference(
            Some(syn::Lifetime::new("'a",proc_macro2::Span::call_site())),
            Box::new(Type::TypePath(syn::parse_str("str").unwrap()))
          ))))
      ),
    ]);
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


