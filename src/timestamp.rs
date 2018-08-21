use {Result, Table};
use sled;
use std::{self, ops};
use unsigned_binary_search::UnsignedBinarySearchKey;

/// An extension to the **Table** trait that ensures each entry in the table is timestamped using
/// another table.
///
/// The resulting table may be searched via both its original key type and using `Self::Timestamp`
/// as a key.
pub trait Timestamped: Table {
    /// The timestamp type used to distinguish between entries.
    type Timestamp: Timestamp;
    /// The table used to timestamp entries into the `Self` table.
    type TimestampTable: Table<Id = Self::Id, Key = Key<Self::Timestamp, Self::Key>, Value = ()>;
    /// Given a reference to a table value, it must provide access to the timestamp.
    fn value_timestamp(value: &Self::Value) -> Self::Timestamp;
}

/// Types that may be used as a timestamp within a **Timestamped** **Table**.
pub trait Timestamp: MinKey + PartialEq + PartialOrd {
    /// Given some timestamp `self`, produce the next consecutive timestamp.
    ///
    /// This is used for the non-inclusive successor table method.
    fn next(&self) -> Self;
}

/// Keys that have a known minimum value.
pub trait MinKey: ::Key {
    /// The minimum possible value that a key be.
    fn min_key() -> Self;
}

/// Types that may be used to describe a timestamp range.
///
/// **Note:** This should be removed once `std::ops::RangeBounds` gets stabilised.
pub trait RangeBounds<T> {
    /// The lower bound. `None` if there is no lower bound.
    fn start_inclusive(&self) -> Option<T>;
    /// The upper bound. `None` if there is no upper bound.
    fn end_exclusive(&self) -> Option<T>;
}

/// A key along with its associated timestamp.
///
/// This type is used as the key with which a **Timestamped** **Table** is indexed.
#[derive(Copy, Clone, Debug, Default, Eq, Hash, PartialEq, PartialOrd, Ord, Deserialize,Serialize)]
pub struct Key<T, K> {
    pub timestamp: T,
    pub key: K,
}

/// Read-only access to a timestamped table within a `sled::Tree`.
#[derive(Debug)]
pub struct Reader<'a, T>
where
    T: Timestamped,
{
    pub(crate) table: ::Writer<'a, T>,
    timestamp_table: ::Writer<'a, T::TimestampTable>,
}

/// Read and write access to a timestamped table within a `sled::Tree`.
pub struct Writer<'a, T>
where
    T: Timestamped,
{
    reader: Reader<'a, T>,
}

/// Iterate over all entries within the table `T` ordered by the timestamp associated with each
/// entry.
pub struct Iter<'a, T>
where
    T: Timestamped,
{
    iter: ::Iter<'a, T::TimestampTable>,
    table: ::Reader<'a, T>,
}

/// Iterate over all entries within the table `T` ordered by the timestamp associated with each
/// entry, as long as that timestamp falls within the given timestamp bounds.
pub struct IterRange<'a, T>
where
    T: Timestamped,
{
    iter: Iter<'a, T>,
    end_exclusive: Option<T::Timestamp>,
}

// Reader implementations.

impl<'a, T> Reader<'a, T>
where
    T: Timestamped,
{
    /// Retrieve a value from the **Tree** if it exists.
    pub fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        self.table.get(key)
    }

    /// Produces read-only access to the table indexed by key rather than by timestamp.
    ///
    /// This is useful when iteration or scanning over keys (rather than timestamp) is desired.
    pub fn by_key(&'a self) -> ::Reader<'a, T> {
        self.table.clone().into()
    }

    /// Return the minimum timestamp entry within the table.
    ///
    /// Note that there may be more than one entry that exists for the returned timestamp.
    pub fn min(&self) -> Result<Option<T::Timestamp>> {
        Ok(self.timestamp_table.min()?.map(|(tk, _)| tk.timestamp))
    }
}

impl<'a, T> Reader<'a, T>
where
    T: Timestamped,
    T::Key: MinKey,
{
    /// Iterate over all entries ordered by the timestamp assicated with each.
    pub fn iter(&self) -> Result<Iter<'a, T>> {
        self.scan(MinKey::min_key())
    }

    /// Iterate over all entries ordered by the timestamp associated with each.
    pub fn scan(&self, timestamp: T::Timestamp) -> Result<Iter<'a, T>> {
        let table = self.table.clone().into();
        let timestamped_key = Key { timestamp, key: MinKey::min_key() };
        let iter = self.timestamp_table.scan(&timestamped_key)?;
        Ok(Iter { table, iter })
    }

    /// Iterate over all entries ordered by the timestamp associated with each as long as it falls
    /// within the given timestamp range.
    pub fn scan_range<R>(&self, range: R) -> Result<IterRange<'a, T>>
    where
        R: RangeBounds<T::Timestamp>,
    {
        let start_inclusive = range.start_inclusive().unwrap_or(MinKey::min_key());
        let end_exclusive = range.end_exclusive();
        let iter = self.scan(start_inclusive)?;
        Ok(IterRange {
            iter,
            end_exclusive,
        })
    }

    /// Return the entry that is equal to or the successor of the given timestamp.
    ///
    /// Returns `None` if no such entry exists.
    ///
    /// This is similar to using the `scan(time).next()` method.
    pub fn succ_incl(&self, timestamp: T::Timestamp) -> Result<Option<T::Timestamp>> {
        let timestamped_key = Key { timestamp, key: MinKey::min_key() };
        Ok(self.timestamp_table.succ_incl(&timestamped_key)?.map(|(tk, _)| tk.timestamp))
    }

    /// Return the entry that is the successor of the given timestamp.
    ///
    /// Returns `None` if no such entry exists.
    ///
    /// This is similar to using the `scan(time).next()` method, but is non-inclusive of the given
    /// key.
    pub fn succ(&self, time: T::Timestamp) -> Result<Option<T::Timestamp>>
    where
        T::Key: PartialEq,
    {
        let next_time = time.next();
        match self.scan(next_time)?.next() {
            None => Ok(None),
            Some(Err(err)) => Err(err),
            Some(Ok((_, v))) => Ok(Some(T::value_timestamp(&v))),
        }
    }
}

impl<'a, T> Reader<'a, T>
where
    T: Timestamped,
    T::Key: UnsignedBinarySearchKey + MinKey,
    Key<T::Timestamp, T::Key>: UnsignedBinarySearchKey,
{
    /// Find and return the entry that is equal to or precedes the given timestamp.
    ///
    /// Returns `None` if no such entry exists.
    pub fn pred_incl(&self, timestamp: T::Timestamp) -> Result<Option<T::Timestamp>> {
        let timestamped_key = Key { timestamp, key: MinKey::min_key() };
        Ok(self.timestamp_table.pred_incl(&timestamped_key)?.map(|(tk, _)| tk.timestamp))
    }

    /// Find and return the entry that precedes the given timestamp.
    ///
    /// Returns `None` if no such entry exists.
    pub fn pred(&self, timestamp: T::Timestamp) -> Result<Option<T::Timestamp>> {
        let timestamped_key = Key { timestamp, key: MinKey::min_key() };
        Ok(self.timestamp_table.pred(&timestamped_key)?.map(|(tk, _)| tk.timestamp))
    }

    /// Find and return the maximum entry within the table.
    ///
    /// This produces the same result as `iter().last()` but much more efficiently.
    pub fn max(&self) -> Result<Option<T::Timestamp>> {
        Ok(self.timestamp_table.max()?.map(|(tk, _)| tk.timestamp))
    }
}

// Writer implementations.

impl<'a, T> Writer<'a, T>
where
    T: Timestamped,
    T::Key: Clone,
{
    /// Set the given **key** to the new **value** with the given **timestamp**.
    pub fn set(&self, key: &T::Key, value: &T::Value) -> Result<()> {
        let timestamp = T::value_timestamp(value);
        let timestamped_key = Key { timestamp, key: key.clone() };
        self.timestamp_table.del(&timestamped_key)?;
        self.table.set(key, value)?;
        self.timestamp_table.set(&timestamped_key, &())?;
        Ok(())
    }

    /// Remove a value from the **Tree** if it exists along with its timestamp entry.
    pub fn del(&self, key: &T::Key) -> Result<Option<T::Value>> {
        if let Some(value) = self.table.del(key)? {
            let timestamp = T::value_timestamp(&value);
            let timestamped_key = Key{ timestamp, key: key.clone() };
            self.timestamp_table.del(&timestamped_key)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

// Trait implementations.

impl<T, K> UnsignedBinarySearchKey for Key<T, K>
where
    T: Timestamp + UnsignedBinarySearchKey,
    K: MinKey + PartialEq + PartialOrd,
{
    type UnsignedInteger = T::UnsignedInteger;
    fn from_unsigned_integer(u: Self::UnsignedInteger) -> Self {
        let timestamp = T::from_unsigned_integer(u);
        let key = MinKey::min_key();
        Key { timestamp, key }
    }
}

impl<T, K> From<(T, K)> for Key<T, K> {
    fn from((timestamp, key): (T, K)) -> Self {
        Key { timestamp, key }
    }
}

impl<'a, T> From<&'a sled::Tree> for Reader<'a, T>
where
    T: Timestamped,
{
    fn from(tree: &'a sled::Tree) -> Self {
        let table = tree.into();
        let timestamp_table = tree.into();
        Reader {
            table,
            timestamp_table,
        }
    }
}

impl<'a, T> From<&'a sled::Tree> for Writer<'a, T>
where
    T: Timestamped,
{
    fn from(tree: &'a sled::Tree) -> Self {
        let reader: Reader<'a, T> = tree.into();
        Writer { reader }
    }
}

impl<'a, T> From<Writer<'a ,T>> for Reader<'a, T>
where
    T: Timestamped,
{
    fn from(w: Writer<'a, T>) -> Self {
        w.reader
    }
}

impl<'a, T> Clone for Reader<'a, T>
where
    T: Timestamped,
{
    fn clone(&self) -> Self {
        let table = self.table.clone();
        let timestamp_table = self.timestamp_table.clone();
        Reader { table, timestamp_table }
    }
}

impl<'a, T> Clone for Writer<'a, T>
where
    T: Timestamped,
{
    fn clone(&self) -> Self {
        let reader = self.reader.clone();
        Writer { reader }
    }
}

impl<'a, T> ops::Deref for Writer<'a, T>
where
    T: Timestamped,
{
    type Target = Reader<'a, T>;
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Timestamped,
{
    type Item = Result<(T::Key, T::Value)>;
    fn next(&mut self) -> Option<Self::Item> {
        let Key { timestamp, key } = match self.iter.next() {
            None => return None,
            Some(Err(err)) => return Some(Err(err)),
            Some(Ok((tk, ()))) => tk,
        };
        let value = match self.table.get(&key) {
            Err(err) => return Some(Err(err)),
            Ok(None) => panic!("timestamp user key invalid - no value found"),
            Ok(Some(value)) => value,
        };
        // Sanity check.
        if timestamp != T::value_timestamp(&value) {
            panic!("timestamp key does not match value's");
        }
        Some(Ok((key, value)))
    }
}

impl<'a, T> Iterator for IterRange<'a, T>
where
    T: Timestamped,
{
    type Item = Result<(T::Key, T::Value)>;
    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = match self.iter.next() {
            None => return None,
            Some(Err(err)) => return Some(Err(err)),
            Some(Ok(kv)) => kv,
        };
        match self.end_exclusive {
            Some(ref end_exclusive) if *end_exclusive <= T::value_timestamp(&value) => None,
            _ => Some(Ok((key, value))),
        }
    }
}

// `RangeBounds` implementations - to be removed once `std::ops::RangeBounds` stabilises.

impl<T> RangeBounds<T> for ops::Range<T>
where
    T: Clone,
{
    fn start_inclusive(&self) -> Option<T> {
        Some(self.start.clone())
    }
    fn end_exclusive(&self) -> Option<T> {
        Some(self.end.clone())
    }
}

impl<T> RangeBounds<T> for ops::RangeFrom<T>
where
    T: Clone,
{
    fn start_inclusive(&self) -> Option<T> {
        Some(self.start.clone())
    }
    fn end_exclusive(&self) -> Option<T> {
        None
    }
}

impl<T> RangeBounds<T> for ops::RangeTo<T>
where
    T: Clone,
{
    fn start_inclusive(&self) -> Option<T> {
        None
    }
    fn end_exclusive(&self) -> Option<T> {
        Some(self.end.clone())
    }
}

impl<T> RangeBounds<T> for ops::RangeFull {
    fn start_inclusive(&self) -> Option<T> {
        None
    }
    fn end_exclusive(&self) -> Option<T> {
        None
    }
}

// Provided MinKey implementations.

impl MinKey for u8 {
    fn min_key() -> Self {
        std::u8::MIN
    }
}

impl MinKey for u16 {
    fn min_key() -> Self {
        std::u16::MIN
    }
}

impl MinKey for u32 {
    fn min_key() -> Self {
        std::u32::MIN
    }
}

impl MinKey for u64 {
    fn min_key() -> Self {
        std::u64::MIN
    }
}

impl MinKey for usize {
    fn min_key() -> Self {
        std::usize::MIN
    }
}

impl MinKey for i8 {
    fn min_key() -> Self {
        std::i8::MIN
    }
}

impl MinKey for i16 {
    fn min_key() -> Self {
        std::i16::MIN
    }
}

impl MinKey for i32 {
    fn min_key() -> Self {
        std::i32::MIN
    }
}

impl MinKey for i64 {
    fn min_key() -> Self {
        std::i64::MIN
    }
}

impl MinKey for isize {
    fn min_key() -> Self {
        std::isize::MIN
    }
}
