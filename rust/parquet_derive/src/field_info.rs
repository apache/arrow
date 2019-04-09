use syn::{
  AngleBracketedGenericArguments, Field, Ident, Lifetime, Path, PathArguments,
  PathSegment, Type as SynType
};

//use proc_macro2::{ Ident as PM2Span, Span };

#[derive(Debug)]
pub struct FieldInfo {
  syn_field: Field,
  ident: Ident,
  field_lifetime: Option<Lifetime>,
  field_type: Ident,
  field_generic_arguments: Vec<FieldInfoGenericArg>,
}

impl FieldInfo {
  pub fn from(f: &syn::Field) -> Self {
    let (field_type, field_lifetime, field_generic_arguments) = match &f.ty {
      SynType::Slice(_) => unimplemented!("unsupported type: Slice"),
      SynType::Array(_) => unimplemented!("unsupported type: Array"),
      SynType::Ptr(_) => unimplemented!("unsupported type: Ptr"),
      SynType::Reference(ref tr) => {
        let (ft, fl, fga) = FieldInfo::extract_type_reference_info(tr);
        (ft, fl, fga)
      },
      SynType::BareFn(_) => unimplemented!("unsupported type: BareFn"),
      SynType::Never(_) => unimplemented!("unsupported type: Never"),
      SynType::Tuple(_) => unimplemented!("unsupported type: Tuple"),
      SynType::Path(tp) => {
        let (ft, fga) = FieldInfo::extract_path_info(&tp);
        (ft, None, fga)
      },
      SynType::TraitObject(_) => unimplemented!("unsupported type: TraitObject"),
      SynType::ImplTrait(_) => unimplemented!("unsupported type: ImplTrait"),
      SynType::Paren(_) => unimplemented!("unsupported type: Paren"),
      SynType::Group(_) => unimplemented!("unsupported type: Group"),
      SynType::Infer(_) => unimplemented!("unsupported type: Infer"),
      SynType::Macro(_) => unimplemented!("unsupported type: Macro"),
      SynType::Verbatim(_) => unimplemented!("unsupported type: Verbatim"),
    };

    FieldInfo {
      syn_field: f.clone(),
      ident: f.ident.clone().expect("must be a named field"),
      field_lifetime,
      field_type,
      field_generic_arguments,
    }
  }

  fn column_writer_variant(&self) -> Path {
    let ftype_string = self.field_type.to_string();
    let ftype_string = if &ftype_string[..] == "Option" {
      let fga: &FieldInfoGenericArg = self
        .field_generic_arguments
        .get(0)
        .expect("must have at least 1 generic argument");
      let field_type = &fga.field_type;

      let field_type_token_stream = quote!{ #field_type };
      field_type_token_stream.to_string()
    } else {
      ftype_string
    };

//    if &ftype_string[..] != "Option" {
//      panic!("blarp: {}", ftype_string);
//    }

    FieldInfo::ident_to_column_writer_variant(&ftype_string[..]).unwrap_or_else(|f| {
      panic!("no column writer variant for {}/{}. err: `{}` ", ftype_string, self.field_type, f)
    })
  }

  fn ident_to_column_writer_variant(id: &str) -> Result<Path, Box<dyn std::error::Error>> {
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

  fn use_as_bytes(id: &str) -> bool {
    match id {
      "u32" => true,
      _ => false,
    }
  }

  fn is_option(&self) -> bool { self.field_type.to_string() == "Option".to_string() }

  /// Takes the parsed data of the struct and emits a valid
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
  /// levels beyond 1.
  pub fn writer_block(&self) -> proc_macro2::TokenStream {
    let definition_levels = self.definition_levels();
    let write_definition_levels = if definition_levels.is_some() {
      quote!{ Some(&definition_levels[..]) }
    } else {
      quote!{ None }
    };
    let vals = self.vals();
    let column_writer_variant = self.column_writer_variant();

    quote!{
      {
        #definition_levels
        #vals
        if let #column_writer_variant(ref mut typed) = column_writer {
            typed.write_batch(&vals[..], #write_definition_levels, None).unwrap();
        }
      }
    }
  }

  pub fn to_writer_snippet_old(&self) -> proc_macro2::TokenStream {
    let field_name = self.ident.clone();
    let field_type = self.field_type.clone();
    let column_writer_variant = self.column_writer_variant();
    let is_option = self.is_option();

    // definition_levels
    // vals
    // typed writer
    // write_batch

    if is_option {
      let definition_levels = quote! {
        let definition_levels : Vec<i16> = self.iter().
          map(|x| if x.#field_name.is_some() { 1 } else { 0 }).
          collect();
      };

      let field_type = &self.field_generic_arguments[0].field_type;
      let field_type_ts = quote!{ #field_type };

      match &field_type_ts.to_string()[..] {
        "str" => quote! {
          {
            #definition_levels
            let vals : Vec<parquet::data_type::ByteArray> = self.iter().
              map(|x| x.#field_name).
              filter_map(|z| {
                if let Some(ref inner) = z {
                    Some((*inner).into())
                } else {
                    None
                }
              }).
              collect();

            if let #column_writer_variant(ref mut typed) = column_writer {
                typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
            }
          }
        },
        // TODO: can this be lumped with str by doing Borrow<str>/AsRef<str> in the
        // ByteArray::from?
        "String" | "Vec" => {
          quote! {
            {
              #definition_levels
              let vals : Vec<parquet::data_type::ByteArray> = self.iter().
                map(|x| &x.#field_name).
                filter_map(|z| {
                  if let Some(ref inner) = z {
                      Some((&inner[..]).into())
                  } else {
                      None
                  }
                }).
                collect();
              if let #column_writer_variant(ref mut typed) = column_writer {
                  typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
              }
            }
          }
        },
        _ => quote! {
          {
            #definition_levels
            let vals : Vec<#field_type> = self.iter().
                    map(|x| x.#field_name).
                    filter(|y| y.is_some()).
                    filter_map(|x| x).
                    collect();

            if let #column_writer_variant(ref mut typed) = column_writer {
                typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
            }
          }
        },
      }
    } else {
      match &self.field_type.to_string()[..] {
        "str" => quote! {
          {
            let vals : Vec<parquet::data_type::ByteArray> = self.iter().map(|x|
                x.#field_name.into()
            ).collect();
            if let #column_writer_variant(ref mut typed) = column_writer {
                typed.write_batch(&vals[..], None, None).unwrap();
            }
          }
        },
        // TODO: can this be lumped with str by doing Borrow<str> in the ByteArray::from?
        "String" => quote! {
          {
            let vals : Vec<parquet::data_type::ByteArray> = self.iter().map(|x|
                (&x.#field_name[..]).into()
            ).collect();
            if let #column_writer_variant(ref mut typed) = column_writer {
                typed.write_batch(&vals[..], None, None).unwrap();
            }
          }
        },
        _ => {
          quote! {
            {
              let vals : Vec<#field_type> = self.iter().map(|x| x.#field_name).collect();
              if let #column_writer_variant(ref mut typed) = column_writer {
                  typed.write_batch(&vals[..], None, None).unwrap();
              }
            }
          }
        },
      }
    }
  }

  fn definition_levels(&self) -> Option<proc_macro2::TokenStream> {
    if self.is_option() {
      let field_name = &self.ident;
      Some(quote! {
        let definition_levels : Vec<i16> = self.iter().
          map(|x| if x.#field_name.is_some() { 1 } else { 0 }).
          collect();
      })
    } else {
      None
    }
  }

  fn field_type_requires_cast(&self) -> bool {
    match self.field_type.to_string().as_ref() {
      "String" => true,
      _ => false
    }
  }

  fn vals(&self) -> proc_macro2::TokenStream {
    if self.is_option() {
      return self.vals_from_option()
    }

    let field_name = &self.ident;

    let (field_type, accessor) : (proc_macro2::TokenStream,proc_macro2::TokenStream) = if self.field_type_requires_cast() {
      (syn::parse2(quote!{ parquet::data_type::ByteArray }).unwrap(),
       syn::parse2( quote!{ (&x.#field_name[..]).into() }).unwrap()
      )
    } else {
      let ftype = &self.field_type;
      (syn::parse2(quote!{ #ftype }).unwrap(),
       syn::parse2(quote!{ x.#field_name }).unwrap()
      )
    };
    let column_writer_variant = self.column_writer_variant();

    quote! {
      let vals : Vec<#field_type> = self.iter().map(|x| #accessor).collect();
    }
  }

  fn vals_from_option(&self) -> proc_macro2::TokenStream {
    let field_type = self.physical_type();
    let field_type_token_stream = quote!{ #field_type };
    let string_field_type = &field_type_token_stream.to_string()[..];

    let field_name = &self.ident;

    let map_accessor = match &string_field_type[..] {
      // Not sure why the physical time comes out like this
      "parquet :: data_type :: ByteArray" => quote! {
        &x.#field_name
      },
      _ => quote! {
        x.#field_name
      }
    };

//    panic!("hi {}/{}", string_field_type, map_accessor.to_string());

    let into_some = match &string_field_type[..] {
      "str" => quote! {
        Some((*inner).into())
      },
      // TODO: can this be lumped with str by doing Borrow<str>/AsRef<str> in the
      // ByteArray::from?
      "String" => quote! {
        Some((&inner[..]).into())
      },
      "Vec" => {
        let fga = &self.field_generic_arguments[0].field_type;
        panic!((quote!{ #fga }).to_string())
      },
      _ => {
        quote! {
          Some(inner)
        }
      },
    };

    quote! {
      let vals : Vec<parquet::data_type::ByteArray> = self.iter().
        map(|x| #map_accessor ).
      filter_map(|z| {
        if let Some(ref inner) = z {
          #into_some
        } else {
          None
        }
      }).
        collect();
    }
  }

  fn vals_are_byte_array(&self) -> bool {
    match &self.field_type.to_string()[..] {
      "str" | "String" => true,
      "Vec" => {
        let path = &self.field_generic_arguments[0].field_generic_args[0].field_type;
        let ts = quote!{ #path };
        let generic_type = ts.to_string();
        unimplemented!()
      }
      _ => false
    }
  }

  fn extract_path_info(
    &syn::TypePath {
      path: Path { ref segments, .. },
      ..
    }: &syn::TypePath,
  ) -> (Ident, Vec<FieldInfoGenericArg>)
  {
    let seg: &PathSegment = segments
      .iter()
      .next()
      .expect("must have at least 1 segment");

    let generic_arg = match &seg.arguments {
      PathArguments::None => vec![],
      PathArguments::AngleBracketed(AngleBracketedGenericArguments {
        ref args, ..
      }) => {
        let generic_argument: &syn::GenericArgument = args.iter().next().unwrap();
        args
          .iter()
          .next()
          .expect("derive only supports one generic argument");
        FieldInfoGenericArg::from_generic_argument(generic_argument)
      },
      PathArguments::Parenthesized(_) => unimplemented!("parenthesized"),
    };

    let retval = (seg.ident.clone(), generic_arg);

    FieldInfo::validate_compatibility(&retval);

    retval
  }

  // Converts the FieldInfo/FieldInfoGenericArg in to the
  // syn::Ident we'll use to build the column writer block
  // u32 -> u32
  // Option<i32> -> i32
  // Option<Vec<u8>> -> parquet::data_type::ByteArray
  // Option<&'a str> -> parquet::data_type::ByteArray
  // Vec<u8> -> parquet::data_type::ByteArray
  fn physical_type(&self) -> Path {
    if self.field_generic_arguments.len() == 0 {
//      self.field_type.clone()
      panic!("huh")
    } else if &self.field_type.to_string()[..] == "Option" {
      let fga_field_type = &self.field_type;

      if self.field_generic_arguments.len() == 0 {
        panic!("option must have a generic type")
      } else if self.field_generic_arguments.len() == 1 {
        let fga = &self.field_generic_arguments[0];
        if fga.field_generic_args.len() == 0 {
          let path = &fga.field_type;
          let ts = quote!{ path };
          match ts.to_string().as_ref() {
            "str" | "String" => {
              syn::parse2(quote!{ parquet::data_type::ByteArray }).unwrap()
            },
            _ => fga.field_type.clone()
          }
//          fga.field_type
        } else if (quote!{ #fga_field_type }).to_string() == "Vec" && fga.field_generic_args.len() == 1 {
          let fga2 = &fga.field_generic_args[0];
          if fga2.field_generic_args.len() != 0 {
            panic!("we don't support more than 2 levels of nested generics");
          } else {
//            panic!("sup buddy\n\n{:#?}\n\n{:#?}\n\n", fga, fga2);
            let path = &fga2.field_type;
            let ts = quote! { #path };
            match ts.to_string().as_ref() {
              "str" | "String" | "u8" => syn::parse2(quote! { parquet::data_type::ByteArray }).unwrap(),
              _ => fga2.field_type.clone()
            }
          }

      } else {
          let ftype = &fga.field_type;
          let genargs = &fga.field_generic_args;
          panic!("can't handle more than 1 field generic args: {}\n\n{:?}\n\n{}\n\n", (quote!{ #ftype }).to_string(), (quote!{ #fga_field_type }).to_string(), fga.field_generic_args.len())
        }
      } else {
        panic!("can't handle more than 1 field generic arg")
      }
    } else if self.field_generic_arguments.len() == 1 {
      let fga = &self.field_generic_arguments[0];
      panic!("hi")
//      if fga.field_generic_args
    } else {
      panic!("can't handle more than 1 field generic arg")
    }
  }

  fn validate_compatibility(&(_, ref fgas): &(Ident, Vec<FieldInfoGenericArg>)) {
    let err = "#[derive(ParquetRecordWriter)] does not support multiple generic args";
    if fgas.len() > 1 {
      unimplemented!("{}", err);
    } else if fgas.len() == 0 {
      return;
    }

    let fga = &fgas[0];
    let fgas2 = &fga.field_generic_args;

    if fgas2.len() > 1 {
      unimplemented!("{}", err)
    } else if fgas2.len() == 0 {
      return;
    }

    let fga2 = &fgas2[0];

    let fga_field_type = fga.field_type_to_string();
    let fga2_field_type = fga2.field_type_to_string();

    if fga_field_type == "Vec" && fga2_field_type != "u8" {
      panic!(
        "we only support Vec<u8>, you are using a Vec<{}>",
        fga2_field_type
      );
    }
  }

  fn extract_type_reference_info(
    &syn::TypeReference {
      ref lifetime,
      ref elem,
      ..
    }: &syn::TypeReference,
  ) -> (Ident, Option<Lifetime>, Vec<FieldInfoGenericArg>)
  {
    if let SynType::Path(ref type_path) = elem.as_ref() {
      let (ident, generic_args) = FieldInfo::extract_path_info(type_path);
      (ident, lifetime.clone(), generic_args)
    } else {
      unimplemented!("unsupported elem {:#?}", elem)
    }
  }
}

#[derive(Debug)]
struct FieldInfoGenericArg {
  field_type: Path,
  field_lifetime: Option<Lifetime>,
  field_generic_args: Vec<FieldInfoGenericArg>,
}

impl FieldInfoGenericArg {
  fn field_type_to_string(&self) -> String {
    let path = &self.field_type;
    let ts = quote!{ #path };
    ts.to_string()
  }

  fn from_generic_argument(arg: &syn::GenericArgument) -> Vec<Self> {
    match arg {
      syn::GenericArgument::Type(SynType::Reference(ref tr)) => {
        let (gen_type, gen_lifetime, fga) = FieldInfo::extract_type_reference_info(tr);
        vec![FieldInfoGenericArg {
          field_type: syn::parse2(quote!{ #gen_type }).unwrap(),
          field_lifetime: gen_lifetime,
          field_generic_args: fga,
        }]
      },
      syn::GenericArgument::Type(SynType::Path(syn::TypePath {
        path: Path { ref segments, .. },
        ..
      })) => {
        let segs: Vec<PathSegment> = segments.clone().into_iter().collect();
        FieldInfoGenericArg::from(segs)
      },
      syn::GenericArgument::Lifetime(_) => unimplemented!("generic arg: lifetime"),
      syn::GenericArgument::Type(_) => unimplemented!("generic arg: only reference/path"),
      syn::GenericArgument::Binding(_) => unimplemented!("generic arg: binding"),
      syn::GenericArgument::Constraint(_) => {
        unimplemented!("generic argument: constraint")
      },
      syn::GenericArgument::Const(_) => unimplemented!("generic argument: const"),
    }
  }

  fn from_path_segment(PathSegment { ident, arguments }: PathSegment) -> Self {
    match arguments {
      PathArguments::None => FieldInfoGenericArg {
        field_type: syn::parse2(quote!{ #ident }).unwrap(),
        field_lifetime: None,
        field_generic_args: vec![],
      },
      PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) => {
        let args_vec: Vec<&syn::GenericArgument> = args.iter().collect();
        if args_vec.len() != 1 {
          println!("only support 1 generic arg");
        }
        let fgas = FieldInfoGenericArg::from_generic_argument(args_vec[0]);
        FieldInfoGenericArg {
          field_type: syn::parse2(quote!{ #ident }).unwrap(),
          field_lifetime: None,
          field_generic_args: fgas,
        }
      },
      PathArguments::Parenthesized(_) => unimplemented!("ho"),
    }
  }

  pub fn from(segments: Vec<PathSegment>) -> Vec<Self> {
    if segments.len() != 1 {
      unimplemented!("parquet derive only supports fields with 1 generic argument")
    }

    segments
      .into_iter()
      .map(|seg| FieldInfoGenericArg::from_path_segment(seg))
      .collect()
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
  fn field_info_vec() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct VecHolder {
        a_vec: Vec<u8>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { a_vec }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Vec }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];
    let exp_gen_type: syn::Path = syn::parse2(quote! { u8 }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);
    assert_eq!(gen.field_lifetime, None);
  }

  #[test]
  fn field_info_option() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct OptionHolder {
        the_option: Option<String>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_option }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Option }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];
    let exp_gen_type: syn::Path = syn::parse2(quote! { String }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);
    assert_eq!(gen.field_lifetime, None);
  }

  #[test]
  fn field_info_borrowed_str() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StrBorrower<'a> {
        the_str: &'a str
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_str }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { str }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    let exp_field_lifetime: syn::Lifetime = syn::parse2(quote! { 'a }).unwrap();
    assert_eq!(fi.field_lifetime.as_ref().unwrap(), &exp_field_lifetime);

    assert_eq!(fi.field_generic_arguments.len(), 0);
  }

  #[test]
  fn field_info_borrowed_string() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        the_string: &'a String
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_string }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { String }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    let exp_field_lifetime: syn::Lifetime = syn::parse2(quote! { 'a }).unwrap();
    assert_eq!(fi.field_lifetime.as_ref().unwrap(), &exp_field_lifetime);

    assert_eq!(fi.field_generic_arguments.len(), 0);
  }

  #[test]
  fn field_info_option_borrowed_str() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        the_option_str: Option<&'a str>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_option_str }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Option }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    assert_eq!(fi.field_lifetime, None);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];

    let exp_gen_type: syn::Path = syn::parse2(quote! { str }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);
    let exp_gen_type: syn::Lifetime = syn::parse2(quote! { 'a }).unwrap();
    assert_eq!(gen.field_lifetime.as_ref().unwrap(), &exp_gen_type);
  }

  #[test]
  fn field_info_borrowed_option_string() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        the_option_str: &'a Option<String>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_option_str }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Option }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    let exp_field_lifetime: syn::Lifetime = syn::parse2(quote! { 'a }).unwrap();
    assert_eq!(fi.field_lifetime.as_ref().unwrap(), &exp_field_lifetime);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];

    let exp_gen_type: syn::Path = syn::parse2(quote! { String }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);
    assert_eq!(gen.field_lifetime, None);
  }

  #[test]
  fn field_info_borrowed_option_borrowed_str() {
    let snippet: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a,'b> {
        the_option_str: &'a Option<&'b str>
      }
    };

    let fields = extract_fields(snippet);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let exp_field_name: syn::Ident = syn::parse2(quote! { the_option_str }).unwrap();
    assert_eq!(fi.ident, exp_field_name);

    let exp_field_type: syn::Ident = syn::parse2(quote! { Option }).unwrap();
    assert_eq!(fi.field_type, exp_field_type);

    let exp_field_lifetime: syn::Lifetime = syn::parse2(quote! { 'a }).unwrap();
    assert_eq!(fi.field_lifetime.as_ref().unwrap(), &exp_field_lifetime);

    assert_eq!(fi.field_generic_arguments.len(), 1);

    let gen: &FieldInfoGenericArg = &fi.field_generic_arguments[0];

    let exp_gen_type: syn::Path = syn::parse2(quote! { str }).unwrap();
    assert_eq!(gen.field_type, exp_gen_type);

    let exp_gen_lifetime: syn::Lifetime = syn::parse2(quote! { 'b }).unwrap();
    assert_eq!(gen.field_lifetime.as_ref().unwrap(), &exp_gen_lifetime);
  }

  #[test]
  fn simple_struct_to_writer_snippet() {
    let struct_def: proc_macro2::TokenStream = quote! {
      struct StringBorrower {
        the_string: String
      }
    };

    let fields = extract_fields(struct_def);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let res = fi.writer_block();

    let writer_snippet: syn::Expr = syn::parse2(fi.writer_block()).unwrap();
    let exp_writer_snippet : syn::Expr = syn::parse2(quote!{
      {
        let vals : Vec<parquet::data_type::ByteArray> = self.iter().map(|x| (&x.the_string[..]).into()).collect();

        if let parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut typed) = column_writer {
            typed.write_batch(&vals[..], None, None).unwrap();
        }
      }
    }).unwrap();

    assert_eq!(writer_snippet, exp_writer_snippet);
  }

  #[test]
  fn option_str_struct_to_writer_snippet() {
    let struct_def: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        optional_str: Option<&'a str>
      }
    };

    let fields = extract_fields(struct_def);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let writer_snippet: syn::Expr = syn::parse2(fi.writer_block()).unwrap();
    let exp_writer_snippet : syn::Expr = syn::parse2(quote!{
      {
        let definition_levels: Vec<i16> = self
          .iter()
          .map(|x| if x.optional_str.is_some() { 1 } else { 0 })
          .collect();

        let vals : Vec<parquet::data_type::ByteArray> = self.iter().
          map(|x| x.optional_str).
          filter_map(|z| {
            if let Some(ref inner) = z {
                Some((*inner).into())
            } else {
                None
            }
          }).
          collect();

        if let parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut typed) = column_writer {
            typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
        }
      }
    }).unwrap();

    assert_eq!(writer_snippet, exp_writer_snippet);
  }

  #[test]
  fn option_string_struct_to_writer_snippet() {
    let struct_def: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        optional_str: Option<String>
      }
    };

    let fields = extract_fields(struct_def);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let writer_snippet: syn::Expr = syn::parse2(fi.to_writer_snippet_old()).unwrap();
    let exp_writer_snippet : syn::Expr = syn::parse2(quote!{
      {
        let definition_levels: Vec<i16> = self
          .iter()
          .map(|x| if x.optional_str.is_some() { 1 } else { 0 })
          .collect();

        let vals : Vec<parquet::data_type::ByteArray> = self.iter().
          map(|x| &x.optional_str).
          filter_map(|z| {
            if let Some(ref inner) = z {
                Some((&inner[..]).into())
            } else {
                None
            }
          }).
          collect();

        if let parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut typed) = column_writer {
            typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
        }
      }
    }).unwrap();

    assert_eq!(writer_snippet, exp_writer_snippet);
  }

  #[test]
  fn option_vec_u8_struct_to_writer_snippet() {
    let struct_def: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        optional_vector: Option<Vec<u8>>
      }
    };

    let fields = extract_fields(struct_def);
    assert_eq!(fields.len(), 1);

    let fi: FieldInfo = FieldInfo::from(&fields[0]);

    let writer_snippet: syn::Expr = syn::parse2(fi.writer_block()).unwrap();
    let exp_writer_snippet : syn::Expr = syn::parse2(quote!{
      {
        let definition_levels: Vec<i16> = self
          .iter()
          .map(|x| if x.optional_vector.is_some() { 1 } else { 0 })
          .collect();

        let vals : Vec<parquet::data_type::ByteArray> = self.iter().
          map(|x| &x.optional_vector).
          filter_map(|z| {
            if let Some(ref inner) = z {
                Some((&inner[..]).into())
            } else {
                None
            }
          }).
          collect();

        if let parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut typed) = column_writer {
            typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
        }
      }
    }).unwrap();

    if writer_snippet != exp_writer_snippet {
      panic!("expected: {}\n\ngot: {}", (quote!{#exp_writer_snippet}).to_string(), (quote!{#writer_snippet}).to_string());
    }
  }

  #[test]
  #[should_panic]
  fn option_vec_i8_struct_to_writer_snippet() {
    let struct_def: proc_macro2::TokenStream = quote! {
      struct StringBorrower<'a> {
        optional_bad_vector: Option<Vec<i8>>
      }
    };

    let fields = extract_fields(struct_def);
    assert_eq!(fields.len(), 1);

    let fi = FieldInfo::from(&fields[0]);

    let writer_snippet: syn::Expr = syn::parse2(fi.writer_block()).unwrap();
    let exp_writer_snippet : syn::Expr = syn::parse2(quote!{
      {
        let definition_levels: Vec<i16> = self
          .iter()
          .map(|x| if x.optional_vector.is_some() { 1 } else { 0 })
          .collect();

        let vals : Vec<parquet::data_type::ByteArray> = self.iter().
          map(|x| &x.optional_vector).
          filter_map(|z| {
            if let Some(ref inner) = z {
                Some((&inner[..]).into())
            } else {
                None
            }
          }).
          collect();

        if let parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut typed) = column_writer {
            typed.write_batch(&vals[..], Some(&definition_levels[..]), None).unwrap();
        }
      }
    }).unwrap();

    assert_eq!(writer_snippet, exp_writer_snippet);
  }

}
