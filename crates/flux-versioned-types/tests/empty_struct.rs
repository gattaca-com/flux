use flux_versioned_types::evolve_struct;

evolve_struct! {
    #[derive(Clone, Debug, PartialEq, Eq, Default)]
    EmptyV1 {}

    #[derive(Clone, Debug, PartialEq, Eq)]
    EmptyV2 {
        add {
            pub field: u32 = 42,
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Default)]
    EmptyV3 {
        remove { field }
    }
}

#[test]
fn test_empty_to_with_field() {
    let v1 = EmptyV1 {};
    let v2: EmptyV2 = v1.into();
    assert_eq!(v2.field, 42);
}

#[test]
fn test_empty_chain() {
    let v1 = EmptyV1 {};
    let v2: EmptyV2 = v1.into();
    let v3: EmptyV3 = v2.into();
    assert_eq!(v3, EmptyV3 {});
}
