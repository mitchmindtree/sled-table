// Make sure that sled allows us to (and continues to) index with keys that don't exactly match.
//
// This is necessary as we search the tree using scan with imprecise keys to iterate over certain
// time ranges.

extern crate sled;

#[test]
fn test_sled_tree_scan_1() {
    let config = sled::ConfigBuilder::new().temporary(true).build();
    let t = sled::Tree::start(config).unwrap();
    t.set(vec![1], vec![10]).unwrap();
    t.set(vec![3], vec![30]).unwrap();
    let mut iter = t.scan(&*vec![2]);
    assert_eq!(iter.next(), Some(Ok((vec![3], vec![30]))));
    assert_eq!(iter.next(), None);
}

#[test]
fn test_sled_tree_scan_2() {
    let config = sled::ConfigBuilder::new().temporary(true).build();
    let t = sled::Tree::start(config).unwrap();
    t.set(vec![1], vec![10]).unwrap();
    t.set(vec![13], vec![130]).unwrap();
    t.set(vec![10, 0], vec![100, 0]).unwrap();
    t.set(vec![10, 20], vec![100, 200]).unwrap();
    t.set(vec![3], vec![30]).unwrap();
    let mut iter = t.scan(&*vec![2]);
    assert_eq!(iter.next(), Some(Ok((vec![3], vec![30]))));
    assert_eq!(iter.next(), Some(Ok((vec![10, 0], vec![100, 0]))));
    assert_eq!(iter.next(), Some(Ok((vec![10, 20], vec![100, 200]))));
    assert_eq!(iter.next(), Some(Ok((vec![13], vec![130]))));
    assert_eq!(iter.next(), None);
}

// Test that the sled tree order is the same as rust byte slide `Ord` impl.
#[test]
fn test_sled_tree_order() {
    let foo = vec![0u8, 232, 64, 93, 13, 54, 67, 111, 124];
    let bar = vec![124u8, 111, 67, 54, 13, 93, 64, 232, 0];
    let baz = vec![54u8, 67, 111, 124, 0, 232, 64, 93, 13];

    let mut vs = vec![foo.clone(), bar.clone(), baz.clone()];
    vs.sort();

    let config = sled::ConfigBuilder::new().temporary(true).build();
    let t = sled::Tree::start(config).unwrap();
    t.set(foo, vec![]).unwrap();
    t.set(bar, vec![]).unwrap();
    t.set(baz, vec![]).unwrap();

    for (vec_elem, tree_elem) in vs.into_iter().zip(&t) {
        println!("{:?}", vec_elem);
        assert_eq!(&vec_elem[..], &tree_elem.unwrap().0[..]);
    }
}
