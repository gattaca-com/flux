#![allow(dead_code)]

use flux::type_hash_derive::{TypeHash, type_hash_lock};
use flux_versioned_types::{evolve_enum, evolve_struct};

#[test]
fn final_attrs_apply_only_to_final_struct() {
    evolve_struct! {
        default_attrs { #[derive(Clone, TypeHash)] }
        final_attrs { #[derive(Debug)] }
        #[type_hash_lock(hash = 12263633316478600747)]
        ValueV1 { pub value: u64 }
        #[type_hash_lock(hash = 17439229365045076247)]
        ValueV2 { add { pub next: u64 = 0 } }
    }
    let value = ValueV2 { value: 1, next: 2 };
    assert_eq!(format!("{value:?}"), "ValueV2 { value: 1, next: 2 }");
}

#[test]
fn final_attrs_coexist_with_custom_enum_defaults() {
    evolve_enum! {
        final_attrs { #[derive(Debug)] }
        default_attrs { #[derive(Clone, Default, TypeHash)] }
        #[type_hash_lock(hash = 9657512568175020449)]
        ValueV1 { #[default] Initial }
        #[type_hash_lock(hash = 17773213065465754729)]
        ValueV2 { add { Next } }
    }
    assert_eq!(format!("{:?}", ValueV2::default()), "Initial");
}
