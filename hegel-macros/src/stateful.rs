use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, ImplItem, ItemImpl};

fn is_rule(a: &Attribute) -> bool {
    a.path().is_ident("rule")
}

fn is_invariant(a: &Attribute) -> bool {
    a.path().is_ident("invariant")
}

pub fn expand_state_machine(mut block: ItemImpl) -> TokenStream {
    let mut rule_names = Vec::new();
    let mut invariant_names = Vec::new();

    for item in &mut block.items {
        if let ImplItem::Fn(method) = item {
            if method.attrs.iter().any(&is_rule) {
                rule_names.push(method.sig.ident.clone());
                method.attrs.retain(|a| !is_rule(a));
            }
            if method.attrs.iter().any(&is_invariant) {
                invariant_names.push(method.sig.ident.clone());
                method.attrs.retain(|a| !is_invariant(a));
            }
        }
    }

    let block_type = &block.self_ty;

    let rule_name_strings: Vec<String> = rule_names.iter().map(|id| id.to_string()).collect();
    let invariant_name_strings: Vec<String> =
        invariant_names.iter().map(|id| id.to_string()).collect();

    quote! {
        #block
        impl ::hegel::stateful::StateMachine for #block_type {
            fn rules(&self) -> Vec<::hegel::stateful::Rule<Self>> {
                vec![ #( ::hegel::stateful::Rule::new(#rule_name_strings, Self::#rule_names) ),* ]
            }
            fn invariants(&self) -> Vec<::hegel::stateful::Rule<Self>> {
                vec![ #( ::hegel::stateful::Rule::new(#invariant_name_strings, Self::#invariant_names) ),* ]
            }
        }
    }
}
