extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate sled;
extern crate sled_table;

use sled_table::Table;
use sled_table::timestamp::Key;

// A unix timestamp representation in nanoseconds.
//
// The number of nanoseconds since 00:00:00 UTC Thursday, 1 January 1970. This means we have
// roughly ~584 years since 1970 until this runs out.
//
// https://en.wikipedia.org/wiki/Unix_time
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
struct UnixNanos(pub i64);

// Trait implementations.

impl sled_table::unsigned_binary_search::UnsignedBinarySearchKey for UnixNanos {
    type UnsignedInteger = u64;
    fn from_unsigned_integer(u: Self::UnsignedInteger) -> Self {
        let i = if u < 9_223_372_036_854_775_808 {
            u as i64 - 9_223_372_036_854_775_807 - 1
        } else {
            (u - 9_223_372_036_854_775_808) as i64
        };
        UnixNanos(i)
    }
}

impl sled_table::timestamp::MinKey for UnixNanos {
    fn min_key() -> Self {
        UnixNanos(::std::i64::MIN)
    }
}

impl sled_table::Timestamp for UnixNanos {
    fn next(&self) -> Self {
        UnixNanos(self.0.checked_add(1).expect("no timestamps left within i64 range"))
    }
}

// ID type for distinguishing between tables at runtime.

#[derive(PartialEq, Serialize, Deserialize)]
#[repr(u8)]
enum TableId {
    Foo = 0,
    FooTimestamp = 1,
}

#[test]
fn test_table_id_size() {
    assert_eq!(std::mem::size_of::<TableId>(), 1);
}

// Data description

// The type used as a key into a table of `Foo`s.
type FooId = u8;

// The data type to be stored within our table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Foo {
    timestamp: UnixNanos,
    data: Vec<u8>,
}

// Table declarations.

// The table used to store `Foo`s.
struct FooTable;

// The table keyed by the timestamp associated with each `Foo`.
struct FooTimestampTable;

// Implementations.

impl Table for FooTable {
    type Id = TableId;
    type Key = FooId;
    type Value = Foo;
    const ID: Self::Id = TableId::Foo;
}

impl Table for FooTimestampTable {
    type Id = TableId;
    type Key = Key<UnixNanos, <FooTable as Table>::Key>;
    type Value = ();
    const ID: Self::Id = TableId::FooTimestamp;
}

impl sled_table::Timestamped for FooTable {
    type Timestamp = UnixNanos;
    type TimestampTable = FooTimestampTable;
    fn value_timestamp(value: &Self::Value) -> UnixNanos {
        value.timestamp
    }
}

// A temporary tree for testing.
fn test_tree() -> sled::Tree {
    let config = sled::ConfigBuilder::new().temporary(true).build();
    sled::Tree::start(config).unwrap()
}

// Tests.

#[test]
fn test_timestamp_table() {
    let t = test_tree();

    // timestamp::Writer::from
    let table = sled_table::timestamp::Writer::<FooTable>::from(&t);

    // timestamp::Writer::set/get
    let a_key = 0;
    let a_value = Foo { timestamp: UnixNanos(8), data: vec![0, 1, 2, 3] };
    let b_key = 4;
    let b_value = Foo { timestamp: UnixNanos(4), data: vec![3, 2, 1, 0] };
    table.set(&a_key, &a_value).unwrap();
    table.set(&b_key, &b_value).unwrap();
    assert_eq!(table.get(&a_key).unwrap().unwrap(), a_value);
    assert_eq!(table.get(&b_key).unwrap().unwrap(), b_value);

    // timestamp::Writer::by_key().iter()
    let mut iter = table.by_key().iter().unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (a_key.clone(), a_value.clone()));
    assert_eq!(iter.next().unwrap().unwrap(), (b_key.clone(), b_value.clone()));
    assert!(iter.next().is_none());

    // timestamp::Writer::iter
    let mut iter = table.iter().unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (b_key.clone(), b_value.clone()));
    assert_eq!(iter.next().unwrap().unwrap(), (a_key.clone(), a_value.clone()));
    assert!(iter.next().is_none());

    // timestamp::Writer::by_key().scan(key)
    let mut iter = table.by_key().scan(&2).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (b_key.clone(), b_value.clone()));
    assert!(iter.next().is_none());

    // timestamp::Writer::scan
    let mut iter = table.scan(UnixNanos(6)).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (a_key.clone(), a_value.clone()));
    assert!(iter.next().is_none());

    // timestamp::Writer::scan_range
    let range = UnixNanos(6)..;
    let mut iter = table.scan_range(range).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (a_key.clone(), a_value.clone()));
    assert!(iter.next().is_none());
    let range = ..UnixNanos(6);
    let mut iter = table.scan_range(range).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (b_key.clone(), b_value.clone()));
    assert!(iter.next().is_none());
    let range = UnixNanos(5)..UnixNanos(8);
    let mut iter = table.scan_range(range).unwrap();
    assert!(iter.next().is_none());
    let mut iter = table.scan_range(..).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (b_key.clone(), b_value.clone()));
    assert_eq!(iter.next().unwrap().unwrap(), (a_key.clone(), a_value.clone()));
    assert!(iter.next().is_none());

    // timestamp::Writer::del
    assert_eq!(table.del(&a_key).unwrap().unwrap(), a_value);
    assert_eq!(table.del(&b_key).unwrap().unwrap(), b_value);
    assert_eq!(table.del(&b_key).unwrap(), None);
}
