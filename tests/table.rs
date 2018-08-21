extern crate sled;
extern crate sled_table;

use sled_table::Table;

// A type that we may use as a test `Table`.
pub struct ByteTable;

// An implementation of `Table` for the unit type, just for testing.
impl Table for ByteTable {
    type Id = u8;
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    const ID: Self::Id = 0;
}

#[test]
fn test_writer() {
    let config = sled::ConfigBuilder::new().temporary(true).build();
    let t = sled::Tree::start(config).unwrap();

    // Writer::from
    let table = sled_table::Writer::<ByteTable>::from(&t);

    // Writer::set/get
    let a_key = vec![1, 2, 3, 4];
    let a_value = vec![5, 6, 7, 8];
    table.set(&a_key, &a_value).unwrap();
    assert_eq!(table.get(&a_key).unwrap().unwrap(), a_value);
    let b_key = vec![1, 2, 3, 0];
    let b_value = vec![0];
    table.set(&b_key, &b_value).unwrap();
    assert_eq!(table.get(&b_key).unwrap().unwrap(), b_value);

    // Writer::iter
    let mut iter = table.iter().unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (b_key.clone(), b_value.clone()));
    assert_eq!(iter.next().unwrap().unwrap(), (a_key.clone(), a_value.clone()));
    assert!(iter.next().is_none());

    // Writer::scan
    let mut iter = table.scan(&vec![1, 2, 3, 1]).unwrap();
    assert_eq!(iter.next().unwrap().unwrap(), (a_key.clone(), a_value.clone()));
    assert!(iter.next().is_none());

    // Writer::del
    assert_eq!(table.del(&a_key).unwrap().unwrap(), a_value);
    assert_eq!(table.del(&b_key).unwrap().unwrap(), b_value);
    assert_eq!(table.del(&b_key).unwrap(), None);
}
