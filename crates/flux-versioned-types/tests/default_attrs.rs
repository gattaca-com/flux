use flux_versioned_types::evolve_struct;

evolve_struct! {
    default_attrs {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(C)]
    }

    DefaultAttrsV1 {
        pub slot: u64,
        pub value: u32,
    }

    DefaultAttrsV2 {
        add {
            pub extra: bool = false,
        }
    }

    #[derive(Default)]  // additional attr on top of default_attrs
    DefaultAttrsV3 {
        add {
            pub count: usize = 0,
        }
    }
}

fn assert_copy<T: Copy>(_: T) {}

#[test]
fn test_default_attrs() {
    let v1 = DefaultAttrsV1 { slot: 100, value: 42 };
    let v2: DefaultAttrsV2 = v1.into();
    assert_eq!(v2.slot, 100);
    assert_eq!(v2.value, 42);
    assert!(!v2.extra);

    let v3: DefaultAttrsV3 = v2.into();
    assert_eq!(v3.slot, 100);
    assert_eq!(v3.value, 42);
    assert!(!v3.extra);
    assert_eq!(v3.count, 0);
}

#[test]
fn test_structs_are_copy() {
    let v1 = DefaultAttrsV1 { slot: 1, value: 2 };
    let v2 = DefaultAttrsV2 { slot: 1, value: 2, extra: true };
    let v3 = DefaultAttrsV3 { slot: 1, value: 2, extra: true, count: 3 };

    assert_copy(v1);
    assert_copy(v2);
    assert_copy(v3);

    assert_eq!(v1.slot, 1);
    assert!(v2.extra);
    assert_eq!(v3.count, 3);
}

#[test]
fn test_default_attrs_additional_derive() {
    // V3 has Default derive in addition to default_attrs
    let v3 = DefaultAttrsV3::default();
    assert_eq!(v3.slot, 0);
    assert_eq!(v3.value, 0);
    assert!(!v3.extra);
    assert_eq!(v3.count, 0);
}
