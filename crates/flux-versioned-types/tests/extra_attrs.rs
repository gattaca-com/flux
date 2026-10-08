#![allow(dead_code)]

use std::hash::Hash;

use flux::type_hash_derive::{TypeHash, type_hash_lock};
use flux_versioned_types::{evolve_enum, versioned_telemetry};

fn assert_defaults_and_extra<T: Copy + std::fmt::Debug + PartialEq + Eq + Hash>() {}

versioned_telemetry!(Extra =>
    extra_attrs { #[derive(Eq, Hash)] }

    #[type_hash_lock(hash = 5662291438731841957)]
    ExtraV1 { pub value: u64 }

    #[type_hash_lock(hash = 15669741485779787215)]
    ExtraV2 { add { pub next: u64 = 0 } }
);

#[test]
fn extra_attrs_extend_the_defaults_on_every_version() {
    assert_defaults_and_extra::<ExtraV1>();
    assert_defaults_and_extra::<ExtraV2>();
}

#[test]
fn extra_attrs_extend_custom_enum_defaults() {
    evolve_enum! {
        #[wire_skip]
        default_attrs { #[derive(Clone, Copy, Debug, PartialEq, TypeHash)] }
        extra_attrs { #[derive(Eq, Hash)] }
        #[type_hash_lock(hash = 6295148186941431606)]
        StateV1 { Initial }
        #[type_hash_lock(hash = 3855814278010476325)]
        StateV2 { add { Next } }
    }
    assert_defaults_and_extra::<StateV1>();
    assert_defaults_and_extra::<StateV2>();
}
