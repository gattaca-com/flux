#![allow(clippy::float_cmp, reason = "testing")]

use flux_versioned_types::evolve_struct;

evolve_struct! {
    #[derive(Clone, Debug, PartialEq, Eq)]
    TestV1 {
        pub a: u32,
        pub b: String,
        pub c: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    TestV2 {
        add {
            pub d: i64 = 0,
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    TestV3 {
        remove { b }
        modify {
            a: u64 = |v| v as u64,
        }
        add {
            pub e: f64 = 1.0,
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    TestV4 {
        remove { c }
        modify {
            a: u128 = |v| v as u128 * 2,
            d: i128 = |v| v as i128,
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    TestV5 {
        add {
            pub f: u64 = |prev: &TestV4| prev.a as u64,
            pub g: i64 = |prev: &TestV4| prev.d as i64,
        }
    }
}

#[test]
fn test_base_struct() {
    let v1 = TestV1 { a: 1, b: "hello".to_string(), c: true };
    assert_eq!(v1.a, 1);
    assert_eq!(v1.b, "hello");
    assert!(v1.c);
}

#[test]
fn test_v1_to_v2() {
    let v1 = TestV1 { a: 1, b: "hello".to_string(), c: true };
    let v2: TestV2 = v1.into();
    assert_eq!(v2.a, 1);
    assert_eq!(v2.b, "hello");
    assert!(v2.c);
    assert_eq!(v2.d, 0);
}

#[test]
fn test_v2_to_v3_with_modify() {
    let v2 = TestV2 { a: 42, b: "world".to_string(), c: false, d: 100 };
    let v3: TestV3 = v2.into();
    assert_eq!(v3.a, 42u64);
    assert!(!v3.c);
    assert_eq!(v3.d, 100);
    assert_eq!(v3.e, 1.0);
}

#[test]
fn test_v3_to_v4_with_multiple_modify() {
    let v3 = TestV3 { a: 10, c: true, d: 50, e: 2.5 };
    let v4: TestV4 = v3.into();
    assert_eq!(v4.a, 20u128);
    assert_eq!(v4.d, 50i128);
    assert_eq!(v4.e, 2.5);
}

#[test]
fn test_chain_v1_to_v4() {
    let v1 = TestV1 { a: 5, b: "chain".to_string(), c: true };
    let v2: TestV2 = v1.into();
    let v3: TestV3 = v2.into();
    let v4: TestV4 = v3.into();
    assert_eq!(v4.a, 10u128);
    assert_eq!(v4.d, 0i128);
    assert_eq!(v4.e, 1.0);
}

#[test]
fn test_v4_to_v5_with_lambda_add() {
    let v4 = TestV4 { a: 20u128, d: 50i128, e: 2.5 };
    let v5: TestV5 = v4.into();
    assert_eq!(v5.a, 20u128);
    assert_eq!(v5.d, 50i128);
    assert_eq!(v5.e, 2.5);
    assert_eq!(v5.f, 20u64);
    assert_eq!(v5.g, 50i64);
}

evolve_struct! {
    #[derive(Clone, Debug, PartialEq, Eq)]
    NonCopyV1 {
        pub name: String,
        pub value: u32,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    NonCopyV2 {
        add {
            pub derived: String = |prev: &NonCopyV1| format!("{}_{}", prev.name, prev.value),
            pub count: usize = |prev: &NonCopyV1| prev.name.len(),
        }
    }
}

#[test]
fn test_non_copy_with_lambda_add() {
    let v1 = NonCopyV1 { name: "test".to_string(), value: 42 };
    let v2: NonCopyV2 = v1.into();
    assert_eq!(v2.name, "test".to_string());
    assert_eq!(v2.value, 42);
    assert_eq!(v2.derived, "test_42".to_string());
    assert_eq!(v2.count, 4);
}
