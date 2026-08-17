use flux_versioned_types::evolve_enum;

// ── base-only (no evolutions) ────────────────────────────────────────────────

evolve_enum! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
    SimpleV1 {
        Slot(u64),
        Value(u32, u32),
        #[default]
        Uninitialized,
    }
}

#[test]
fn test_base_enum() {
    let a = SimpleV1::Slot(42);
    let b = SimpleV1::Value(1, 2);
    let c = SimpleV1::Uninitialized;
    assert_eq!(a, SimpleV1::Slot(42));
    assert_eq!(b, SimpleV1::Value(1, 2));
    assert_eq!(c, SimpleV1::Uninitialized);
}

// ── add variants ─────────────────────────────────────────────────────────────

evolve_enum! {
    default_attrs {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
    }

    AddTestV1 {
        Existing(u32),
        #[default]
        Uninitialized,
    }

    AddTestV2 {
        add {
            NewUnit,
            NewTuple(u64, bool),
        }
    }
}

#[test]
fn test_add_variants() {
    let v1 = AddTestV1::Existing(7);
    let v2: AddTestV2 = v1.into();
    assert_eq!(v2, AddTestV2::Existing(7));

    let v1_uninit = AddTestV1::Uninitialized;
    let v2_uninit: AddTestV2 = v1_uninit.into();
    assert_eq!(v2_uninit, AddTestV2::Uninitialized);

    // Added variants exist and are constructible.
    let _ = AddTestV2::NewUnit;
    let _ = AddTestV2::NewTuple(1, true);
}

// ── remove variants (maps to Default::default()) ─────────────────────────────

evolve_enum! {
    default_attrs {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
    }

    RemoveTestV1 {
        Keep(u32),
        Remove1(u64),
        Remove2(u8, u8),
        #[default]
        Uninitialized,
    }

    RemoveTestV2 {
        remove { Remove1, Remove2 }
    }
}

#[test]
fn test_remove_variants() {
    let kept: RemoveTestV2 = RemoveTestV1::Keep(99).into();
    assert_eq!(kept, RemoveTestV2::Keep(99));

    let removed_single: RemoveTestV2 = RemoveTestV1::Remove1(10).into();
    assert_eq!(removed_single, RemoveTestV2::default());

    let removed_multi: RemoveTestV2 = RemoveTestV1::Remove2(1, 2).into();
    assert_eq!(removed_multi, RemoveTestV2::default());
}

// ── remove with rename (from pattern) ────────────────────────────────────────

evolve_enum! {
    default_attrs {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
    }

    RenameTestV1 {
        OldName(u32),
        MultiOld(u32, u64),
        UnitOld,
        #[default]
        Uninitialized,
    }

    RenameTestV2 {
        remove {
            OldName,
            MultiOld,
            UnitOld,
        }
        add {
            NewName(u64) from OldName = |v: u32| v as u64 * 2,
            MultiNew(u32, u64) from MultiOld,
            UnitNew from UnitOld,
        }
    }
}

#[test]
fn test_rename_single_field() {
    let v1 = RenameTestV1::OldName(42);
    let v2: RenameTestV2 = v1.into();
    assert_eq!(v2, RenameTestV2::NewName(84u64));
}

#[test]
fn test_rename_multi_field() {
    let v1 = RenameTestV1::MultiOld(1, 2);
    let v2: RenameTestV2 = v1.into();
    assert_eq!(v2, RenameTestV2::MultiNew(1, 2));
}

#[test]
fn test_rename_unit() {
    let v1 = RenameTestV1::UnitOld;
    let v2: RenameTestV2 = v1.into();
    assert_eq!(v2, RenameTestV2::UnitNew);
}

// ── modify variants
// ───────────────────────────────────────────────────────────

evolve_enum! {
    default_attrs {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
    }

    ModifyTestV1 {
        SingleField(u32),
        MultiField(u32),
        UnitVariant,
        #[default]
        Uninitialized,
    }

    ModifyTestV2 {
        modify {
            SingleField(u64) = |v: u32| v as u64 * 2,
            MultiField(u64, u64) = |a: u32| (a as u64, a as u64 + 2),
            UnitVariant,
        }
    }
}

#[test]
fn test_modify_single_field() {
    let v1 = ModifyTestV1::SingleField(5);
    let v2: ModifyTestV2 = v1.into();
    assert_eq!(v2, ModifyTestV2::SingleField(10u64));
}

#[test]
fn test_modify_multi_field() {
    let v1 = ModifyTestV1::MultiField(3);
    let v2: ModifyTestV2 = v1.into();
    assert_eq!(v2, ModifyTestV2::MultiField(3, 5));
}

#[test]
fn test_modify_unit_passthrough() {
    let v1 = ModifyTestV1::UnitVariant;
    let v2: ModifyTestV2 = v1.into();
    assert_eq!(v2, ModifyTestV2::UnitVariant);
}

// ── combined: add + remove + modify ──────────────────────────────────────────

evolve_enum! {
    default_attrs {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
    }

    ComboV1 {
        SlotInfo(u32),
        LegacyRelay(u8),
        #[default]
        Uninitialized,
    }

    ComboV2 {
        add {
            NewFeature(u64),
        }
        remove { LegacyRelay }
        modify {
            SlotInfo(u64) = |v: u32| v as u64,
        }
    }

    ComboV3 {
        add {
            #[default]
            AnotherDefault,
        }
        remove { Uninitialized }
    }
}

#[test]
fn test_combo_v1_to_v2() {
    let modified: ComboV2 = ComboV1::SlotInfo(100).into();
    assert_eq!(modified, ComboV2::SlotInfo(100u64));

    let removed: ComboV2 = ComboV1::LegacyRelay(5).into();
    assert_eq!(removed, ComboV2::default());
}

#[test]
fn test_combo_v2_to_v3() {
    let kept: ComboV3 = ComboV2::SlotInfo(42).into();
    assert_eq!(kept, ComboV3::SlotInfo(42));

    let uninit: ComboV3 = ComboV2::Uninitialized.into();
    assert_eq!(uninit, ComboV3::AnotherDefault);
}

// ── explicit discriminants (relay-ID pattern)
// ─────────────────────────────────

evolve_enum! {
    default_attrs {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
        #[repr(u8)]
    }

    RelayIdV1 {
        Titan = 1,
        Flashbots = 2,
        #[default]
        Unknown = 255,
    }

    RelayIdV2 {
        add { Beaver = 3 }
        remove { Titan }
    }

    RelayIdV3 {
        remove { Beaver }
        add {
            Beaver = 4 from Beaver,
        }
    }
}

#[test]
fn test_relay_id_discriminants_preserved() {
    assert_eq!(RelayIdV1::Titan as u8, 1);
    assert_eq!(RelayIdV1::Flashbots as u8, 2);
    assert_eq!(RelayIdV1::Unknown as u8, 255);

    // Kept variants carry their discriminant into V2.
    assert_eq!(RelayIdV2::Flashbots as u8, 2);
    assert_eq!(RelayIdV2::Unknown as u8, 255);

    // New variant has its own discriminant.
    assert_eq!(RelayIdV2::Beaver as u8, 3);

    // Conversion: kept variant maps correctly.
    let v2: RelayIdV2 = RelayIdV1::Flashbots.into();
    assert_eq!(v2, RelayIdV2::Flashbots);

    // Removed variant maps to Default.
    let v2_removed: RelayIdV2 = RelayIdV1::Titan.into();
    assert_eq!(v2_removed, RelayIdV2::default());

    // New variant with the same name has new discriminant.
    let v3: RelayIdV3 = RelayIdV2::Beaver.into();
    assert_eq!(v3, RelayIdV3::Beaver);
    assert_eq!(v3 as u8, 4);
}
