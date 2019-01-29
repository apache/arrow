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

#![recursion_limit = "300"]

extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro2::{Span, TokenStream};
use std::iter;
use syn::{
    punctuated::Punctuated, spanned::Spanned, Attribute, Data, DataEnum, DeriveInput, Error, Field,
    Fields, Ident, Lit, LitStr, Meta, NestedMeta, TypeParam, WhereClause,
};

#[proc_macro_derive(Deserialize, attributes(parquet))]
pub fn parquet_deserialize(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    syn::parse::<DeriveInput>(input)
        .and_then(|ast| match ast.data {
            Data::Struct(ref s) => match s.fields {
                Fields::Named(ref fields) => impl_struct(&ast, &fields.named),
                Fields::Unit => impl_struct(&ast, &Punctuated::new()),
                Fields::Unnamed(ref fields) => impl_tuple_struct(&ast, &fields.unnamed),
            },
            Data::Enum(ref e) => impl_enum(&ast, e),
            Data::Union(_) => unimplemented!("#[derive(Deserialize)] doesn't work with unions"),
        })
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

fn impl_struct(
    ast: &DeriveInput,
    fields: &Punctuated<Field, Token![,]>,
) -> Result<TokenStream, Error> {
    let name = &ast.ident;
    let schema_name = Ident::new(&format!("{}Schema", name), Span::call_site());
    let reader_name = Ident::new(&format!("{}Reader", name), Span::call_site());

    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let mut where_clause = where_clause
        .map(Clone::clone)
        .unwrap_or_else(|| WhereClause {
            where_token: <Token![where]>::default(),
            predicates: Punctuated::new(),
        });
    for TypeParam { ident, .. } in ast.generics.type_params() {
        where_clause
            .predicates
            .push(syn::parse2(quote! { #ident: Deserialize }).unwrap());
    }
    let mut where_clause_with_debug = where_clause.clone();
    for TypeParam { ident, .. } in ast.generics.type_params() {
        where_clause_with_debug
            .predicates
            .push(syn::parse2(quote! { <#ident as Deserialize>::Schema: Debug }).unwrap());
    }

    let field_renames = fields
        .iter()
        .map(|field| {
            let mut rename = None;
            for meta_items in field.attrs.iter().filter_map(get_parquet_meta_items) {
                for meta_item in meta_items {
                    match meta_item {
                        // Parse `#[parquet(rename = "foo")]`
                        NestedMeta::Meta(Meta::NameValue(ref m)) if m.ident == "rename" => {
                            let s = get_lit_str(&m.ident, &m.ident, &m.lit)?;
                            if rename.is_some() {
                                return Err(Error::new_spanned(
                                    &m.ident,
                                    "duplicate parquet attribute `rename`",
                                ));
                            }
                            rename = Some(s.clone());
                        }
                        NestedMeta::Meta(ref meta_item) => {
                            return Err(Error::new_spanned(
                                meta_item.name(),
                                format!("unknown parquet field attribute `{}`", meta_item.name()),
                            ));
                        }
                        NestedMeta::Literal(ref lit) => {
                            return Err(Error::new_spanned(
                                lit,
                                "unexpected literal in parquet field attribute",
                            ));
                        }
                    }
                }
            }
            Ok(rename.unwrap_or_else(|| {
                LitStr::new(&field.ident.as_ref().unwrap().to_string(), field.span())
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let field_renames1 = &field_renames;
    let field_renames2 = &field_renames;

    let field_names = fields
        .iter()
        .map(|field| field.ident.as_ref().unwrap())
        .collect::<Vec<_>>();
    let field_names1 = &field_names;
    let field_names2 = &field_names;

    let field_types = fields.iter().map(|field| &field.ty).collect::<Vec<_>>();
    let field_types1 = &field_types;

    let name1 = iter::repeat(name).take(fields.len());

    let gen = quote! {
        use _parquet::{
            basic::Repetition,
            column::reader::ColumnReader,
            errors::ParquetError,
            record::{Deserialize, Schema, reader::Reader, _private::DisplaySchemaGroup},
            schema::types::{ColumnPath, Type},
        };
        use ::std::{collections::HashMap, cmp::PartialEq, fmt::{self, Debug}, result::Result, string::String, vec::Vec};

        struct #schema_name #impl_generics #where_clause {
            #(#field_names1: <#field_types1 as Deserialize>::Schema,)*
        }
        impl #impl_generics Debug for #schema_name #ty_generics #where_clause_with_debug {
            fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
                f.debug_struct(stringify!(#schema_name))
                    #(.field(stringify!(#field_names1), &self.#field_names2))*
                    .finish()
            }
        }
        impl #impl_generics Schema for #schema_name #ty_generics #where_clause {
            fn fmt(self_: Option<&Self>, r: Option<Repetition>, name: Option<&str>, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
                let mut printer = DisplaySchemaGroup::new(r, name, None, f);
                #(
                    printer.field(Some(stringify!(#field_names1)), self_.map(|self_|&self_.#field_names2));
                )*
                printer.finish()
            }
        }
        struct #reader_name #impl_generics #where_clause {
            #(#field_names1: <#field_types1 as Deserialize>::Reader,)*
        }
        impl #impl_generics Reader for #reader_name #ty_generics #where_clause {
            type Item = #name #ty_generics;

            fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item, ParquetError> {
                Result::Ok(#name {
                    #(#field_names1: self.#field_names2.read(def_level, rep_level)?,)*
                })
            }
            fn advance_columns(&mut self) -> Result<(), ParquetError> {
                #(self.#field_names1.advance_columns()?;)*
                Result::Ok(())
            }
            fn has_next(&self) -> bool {
                #(if true { self.#field_names1.has_next() } else)*
                {
                    true
                }
            }
            fn current_def_level(&self) -> i16 {
                #(if true { self.#field_names1.current_def_level() } else)*
                {
                    panic!("Current definition level: empty group reader")
                }
            }
            fn current_rep_level(&self) -> i16 {
                #(if true { self.#field_names1.current_rep_level() } else)*
                {
                    panic!("Current repetition level: empty group reader")
                }
            }
        }
        impl #impl_generics Deserialize for #name #ty_generics #where_clause {
            type Schema = #schema_name #ty_generics;
            type Reader = #reader_name #ty_generics;

            fn parse(schema: &Type, repetition: Option<Repetition>) -> Result<(String, Self::Schema), ParquetError> {
                if schema.is_group() && repetition == Some(Repetition::REQUIRED) {
                    let fields = schema.get_fields().iter().map(|field|(field.name(),field)).collect::<HashMap<_,_>>();
                    let schema_ = #schema_name{
                        #(#field_names1: fields.get(#field_renames1).ok_or(ParquetError::General(format!("Struct \"{}\" has field \"{}\" not in the schema", stringify!(#name1), #field_renames2))).and_then(|x|<#field_types1 as Deserialize>::parse(&**x, Some(x.get_basic_info().repetition())))?.1,)*
                    };
                    return Result::Ok((schema.name().to_owned(), schema_))
                }
                Result::Err(ParquetError::General(format!("Struct \"{}\" is not in the schema", stringify!(#name))))
            }
            fn reader(schema: &Self::Schema, mut path: &mut Vec<String>, def_level: i16, rep_level: i16, paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize) -> Self::Reader {
                #(
                    path.push(#field_renames1.to_owned());
                    let #field_names1 = <#field_types1 as Deserialize>::reader(&schema.#field_names2, path, def_level, rep_level, paths, batch_size);
                    path.pop().unwrap();
                )*
                #reader_name { #(#field_names1,)* }
            }
        }
    };

    Ok(wrap_in_const("DESERIALIZE", name, gen))
}

fn impl_tuple_struct(
    ast: &DeriveInput,
    fields: &Punctuated<Field, Token![,]>,
) -> Result<TokenStream, Error> {
    let _name = &ast.ident;
    let _schema_name = Ident::new(&format!("{}Schema", _name), Span::call_site());
    let _reader_name = Ident::new(&format!("{}Reader", _name), Span::call_site());

    let (_impl_generics, _ty_generics, _where_clause) = ast.generics.split_for_impl();

    for field in fields.iter() {
        for meta_items in field.attrs.iter().filter_map(get_parquet_meta_items) {
            for meta_item in meta_items {
                match meta_item {
                    NestedMeta::Meta(ref meta_item) => {
                        return Err(Error::new_spanned(
                            meta_item.name(),
                            format!("unknown parquet field attribute `{}`", meta_item.name()),
                        ));
                    }
                    NestedMeta::Literal(ref lit) => {
                        return Err(Error::new_spanned(
                            lit,
                            "unexpected literal in parquet field attribute",
                        ));
                    }
                }
            }
        }
    }

    unimplemented!("#[derive(Deserialize)] on tuple structs not yet implemented")
}

fn impl_enum(ast: &DeriveInput, data: &DataEnum) -> Result<TokenStream, Error> {
    if data.variants.is_empty() {
        return Err(Error::new_spanned(
            ast,
            "#[derive(Deserialize)] cannot be implemented for enums with zero variants",
        ));
    }
    for v in data.variants.iter() {
        if v.fields.iter().len() == 0 {
            return Err(Error::new_spanned(
                v,
                "#[derive(Deserialize)] cannot be implemented for enums with non-unit variants",
            ));
        }
    }

    unimplemented!("#[derive(Deserialize)] on enums not yet implemented")
}

fn get_parquet_meta_items(attr: &Attribute) -> Option<Vec<NestedMeta>> {
    if attr.path.segments.len() == 1 && attr.path.segments[0].ident == "parquet" {
        match attr.interpret_meta() {
            Some(Meta::List(ref meta)) => Some(meta.nested.iter().cloned().collect()),
            _ => {
                // TODO: produce an error
                None
            }
        }
    } else {
        None
    }
}

fn get_lit_str<'a>(
    attr_name: &Ident,
    meta_item_name: &Ident,
    lit: &'a Lit,
) -> Result<&'a LitStr, Error> {
    if let Lit::Str(ref lit) = *lit {
        Ok(lit)
    } else {
        Err(Error::new_spanned(
            lit,
            format!(
                "expected parquet {} attribute to be a string: `{} = \"...\"`",
                attr_name, meta_item_name
            ),
        ))
    }
}

fn wrap_in_const(trait_: &str, ty: &Ident, code: TokenStream) -> TokenStream {
    let dummy_const = Ident::new(
        &format!("_IMPL_{}_FOR_{}", trait_, unraw(ty)),
        Span::call_site(),
    );

    quote! {
        #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
        const #dummy_const: () = {
            #[allow(unknown_lints)]
            #[cfg_attr(feature = "cargo-clippy", allow(useless_attribute))]
            #[allow(rust_2018_idioms)]
            extern crate parquet as _parquet;
            #code
        };
    }
}

fn unraw(ident: &Ident) -> String {
    ident.to_string().trim_start_matches("r#").to_owned()
}
