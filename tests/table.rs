extern crate sled;
extern crate sled_table;

use sled_table::Table;
use std::mem;

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
    let tree = sled::Tree::start(config).unwrap();

    // Writer::from
    let table = sled_table::Writer::<ByteTable>::from(&tree);

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
    assert_eq!(
        iter.next().unwrap().unwrap(),
        (b_key.clone(), b_value.clone())
    );
    assert_eq!(
        iter.next().unwrap().unwrap(),
        (a_key.clone(), a_value.clone())
    );
    assert!(iter.next().is_none());

    // Writer::scan
    let mut iter = table.scan(&vec![1, 2, 3, 1]).unwrap();
    assert_eq!(
        iter.next().unwrap().unwrap(),
        (a_key.clone(), a_value.clone())
    );
    assert!(iter.next().is_none());

    // Writer::del
    assert_eq!(table.del(&a_key).unwrap().unwrap(), a_value);
    assert_eq!(table.del(&b_key).unwrap().unwrap(), b_value);
    assert_eq!(table.del(&b_key).unwrap(), None);
}

#[test]
fn test_table_size_bytes() {
    let config = sled::ConfigBuilder::new().temporary(true).build();
    let tree = sled::Tree::start(config).unwrap();
    let table = sled_table::Writer::<ByteTable>::from(&tree);

    // Size with one entry.
    let a_key = vec![1, 2, 3, 4];
    let a_value = vec![5, 6, 7, 8];
    table.set(&a_key, &a_value).unwrap();
    let expected_a = mem::size_of::<<ByteTable as Table>::Id>() // a_key prepended table ID
        + a_key.len()
        + mem::size_of::<usize>() // length of a_value
        + a_value.len();
    assert_eq!(table.size_bytes().unwrap(), expected_a);

    // Size with two entries.
    let b_key = vec![1, 2, 3, 0];
    let b_value = vec![0];
    table.set(&b_key, &b_value).unwrap();
    let expected_b = mem::size_of::<<ByteTable as Table>::Id>() // a_key prepended table ID
        + a_key.len()
        + mem::size_of::<usize>() // length of a_value
        + a_value.len()
        + mem::size_of::<<ByteTable as Table>::Id>() // b_key prepended table ID
        + b_key.len()
        + mem::size_of::<usize>() // length of b_value
        + b_value.len();
    assert_eq!(table.size_bytes().unwrap(), expected_b);

    // Size with second entry removed.
    table.del(&b_key).unwrap();
    assert_eq!(table.size_bytes().unwrap(), expected_a);
    assert_eq!(table.size_bytes().unwrap(), sled_table::tree_size_bytes(&tree).unwrap());
}
