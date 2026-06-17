//! FlowDB derive macros.
//!
//! Provides `#[derive(ObjectStore)]` for generating a [`StoreDef`] from a
//! struct definition with field-level index annotations.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Lit};

/// Derive the [`ObjectStore`] trait for a struct.
///
/// Generates an implementation of `ObjectStore::store_def()` based on the
/// struct's `#[store(...)]` container attribute and `#[index(...)]` field
/// attributes.
///
/// # Container Attribute
///
/// `#[store(key_path = "...")]` — required. Specifies the primary key field
/// path. Options:
/// - `name = "..."` — override the store name (defaults to the struct name)
/// - `auto_increment` — enable auto-increment primary keys
///
/// ```ignore
/// #[derive(ObjectStore)]
/// #[store(key_path = "id", auto_increment)]
/// struct Log { ... }
/// ```
///
/// # Field Attributes
///
/// `#[index(...)]` on a field creates a secondary index. Accepted options:
/// - `unique` — create a unique index
/// - `name = "..."` — custom index name (defaults to the field name)
///
/// ```ignore
/// #[derive(ObjectStore)]
/// #[store(key_path = "id")]
/// struct User {
///     #[index(unique)]
///     email: String,
///     #[index]
///     age: u32,
///     city: String,          // no index
/// }
/// ```
#[proc_macro_derive(ObjectStore, attributes(store, index))]
pub fn derive_object_store(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let store_name = name.to_string();

    let mut key_path: Option<String> = None;
    let mut auto_increment = false;
    let mut store_name = store_name; // override via #[store(name = "...")]

    for attr in &input.attrs {
        if attr.path().is_ident("store") {
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("key_path") {
                    let value: Lit = meta.value()?.parse()?;
                    if let Lit::Str(s) = value {
                        key_path = Some(s.value());
                    }
                } else if meta.path.is_ident("auto_increment") {
                    auto_increment = true;
                } else if meta.path.is_ident("name") {
                    let value: Lit = meta.value()?.parse()?;
                    if let Lit::Str(s) = value {
                        store_name = s.value();
                    }
                }
                Ok(())
            });
        }
    }

    let key_path = key_path.expect("#[store(key_path = \"...\")] is required");

    let mut indexes = Vec::new();

    if let syn::Data::Struct(data) = &input.data {
        for field in &data.fields {
            let field_name = field
                .ident
                .as_ref()
                .map(|i| i.to_string())
                .expect("named fields only");

            for attr in &field.attrs {
                if attr.path().is_ident("index") {
                    let mut unique = false;
                    let mut index_name = field_name.clone();

                    let _ = attr.parse_nested_meta(|meta| {
                        if meta.path.is_ident("unique") {
                            unique = true;
                        } else if meta.path.is_ident("name") {
                            let value: Lit = meta.value()?.parse()?;
                            if let Lit::Str(s) = value {
                                index_name = s.value();
                            }
                        }
                        Ok(())
                    });

                    indexes.push((index_name, field_name.clone(), unique));
                }
            }
        }
    }

    let index_calls: Vec<_> = indexes
        .iter()
        .map(|(idx_name, field_name, unique)| {
            let u = *unique;
            quote! {
                .with_index(#idx_name, &[#field_name], #u)
            }
        })
        .collect();

    let auto_inc = if auto_increment {
        quote! { .with_auto_increment() }
    } else {
        quote! {}
    };

    let expanded = quote! {
        impl flowdb::jsondb::ObjectStore for #name {
            fn store_def() -> flowdb::jsondb::StoreSchema {
                flowdb::jsondb::StoreSchema::new(#store_name, #key_path)
                    #(#index_calls)*
                    #auto_inc
            }
        }
    };

    expanded.into()
}
