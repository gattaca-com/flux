#![allow(dead_code)]

use flux::type_hash_derive::type_hash_lock;
use flux_versioned_types::{versioned_enum, versioned_telemetry};

macro_rules! listing {
    ($tag:literal ; name = $name:literal, latest = $latest:ty, versions = [$($version:ty),*], $list:ident = [$($member:ident),*]) => {
        (
            $tag,
            $name,
            ::core::mem::size_of::<$latest>(),
            [$(::core::mem::size_of::<$version>()),*],
            stringify!($list),
            [$(stringify!($member)),*],
        )
    };
}

versioned_telemetry!(Finished, persist = "Sim.Finished" =>
    final_attrs { #[export_versions] }
    #[type_hash_lock(hash = 10926953839156428172)]
    FinishedV1 { pub a: u32, pub old: u32 }
    #[type_hash_lock(hash = 17584979675724641136)]
    FinishedV2 { remove { old } add { pub b: u32 = 0, pub c: u64 = 0 } }
);

versioned_enum!(#[wire_name = "Sim.Side"] Side =>
    final_attrs { #[export_versions] }
    #[type_hash_lock(hash = 17816951152586584993)]
    SideV1 { Buy, Sell }
    #[type_hash_lock(hash = 3238629743629312039)]
    SideV2 { add { Hold } }
);

#[test]
fn export_versions_lists_versions_oldest_first_and_the_newest_members() {
    assert_eq!(
        __versions_Sim_Finished!(listing, "struct"),
        ("struct", "Sim.Finished", 16, [8, 16], "fields", ["a", "b", "c"])
    );
    assert_eq!(
        __versions_Sim_Side!(listing, "enum"),
        ("enum", "Sim.Side", 1, [1, 1], "variants", ["Buy", "Sell", "Hold"])
    );
}
