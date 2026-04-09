use flux::{type_hash::TypeHash, type_hash_derive::TypeHash};

// -- name override produces same hash as original -----------------------------

#[derive(Clone, Copy, Debug, TypeHash)]
#[repr(C)]
struct Foo {
    pub a: u32,
    pub b: u64,
}

#[derive(Clone, Copy, Debug, TypeHash)]
#[type_hash(skip_typename_on_derive, name = "Foo")]
#[repr(C)]
struct FooV1 {
    pub a: u32,
    pub b: u64,
}

const _: () = assert!(Foo::TYPE_HASH == FooV1::TYPE_HASH);

// -- different fields still differ --------------------------------------------

#[derive(Clone, Copy, Debug, TypeHash)]
#[type_hash(skip_typename_on_derive, name = "Foo")]
#[repr(C)]
struct FooV2 {
    pub a: u32,
    pub b: u64,
    pub c: bool,
}

const _: () = assert!(Foo::TYPE_HASH != FooV2::TYPE_HASH);

// -- without override differs -------------------------------------------------

#[derive(Clone, Copy, Debug, TypeHash)]
#[type_hash(skip_typename_on_derive)]
#[repr(C)]
struct FooNoOverride {
    pub a: u32,
    pub b: u64,
}

const _: () = assert!(Foo::TYPE_HASH != FooNoOverride::TYPE_HASH);

// -- enum name override produces same hash ------------------------------------

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, TypeHash)]
#[repr(u8)]
enum MyEnum {
    A,
    B(u32),
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, TypeHash)]
#[type_hash(skip_typename_on_derive, name = "MyEnum")]
#[repr(u8)]
enum MyEnumV1 {
    A,
    B(u32),
}

const _: () = assert!(MyEnum::TYPE_HASH == MyEnumV1::TYPE_HASH);

// -- different variants still differ ------------------------------------------

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, TypeHash)]
#[type_hash(skip_typename_on_derive, name = "MyEnum")]
#[repr(u8)]
enum MyEnumV2 {
    A,
    B(u32),
    C,
}

const _: () = assert!(MyEnum::TYPE_HASH != MyEnumV2::TYPE_HASH);

// -- without override differs -------------------------------------------------

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, TypeHash)]
#[type_hash(skip_typename_on_derive)]
#[repr(u8)]
enum MyEnumNoOverride {
    A,
    B(u32),
}

const _: () = assert!(MyEnum::TYPE_HASH != MyEnumNoOverride::TYPE_HASH);

// -- composite: name-overridden fields produce identical hash -----------------

#[derive(Clone, Copy, Debug, TypeHash)]
#[repr(C)]
struct Composite {
    pub foo: Foo,
    pub e: MyEnum,
}

#[derive(Clone, Copy, Debug, TypeHash)]
#[type_hash(skip_typename_on_derive, name = "Composite")]
#[repr(C)]
struct CompositeV1 {
    pub foo: FooV1,
    pub e: MyEnumV1,
}

const _: () = assert!(Composite::TYPE_HASH == CompositeV1::TYPE_HASH);
