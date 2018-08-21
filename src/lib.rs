//! A wrapper around `&sled::Tree` which provides an API around a single **Table** within the tree.

extern crate bincode;
extern crate bytekey;
extern crate serde;
#[macro_use] extern crate serde_derive;
pub extern crate sled;

use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::{fmt, ops};
use std::marker::PhantomData;
use unsigned_binary_search::UnsignedBinarySearchKey;

pub use self::reversible::Reversible;
pub use self::timestamp::{Timestamp, Timestamped};

pub mod reversible;
pub mod timestamp;
pub mod unsigned_binary_search;

/// A single table within a `sled::Tree`.
pub trait Table {
    /// The type used to distinguish tables from one another.
    type Id: Id;
    /// The type used as a key into the table.
    type Key: Key;
    /// The type used as the value associated with a key.
    type Value: Value;
    /// A constant, unique identifier that distinguishes the table from all others at runtime.
    const ID: Self::Id;
}

/// Types that may be used as a **Id** to distinguish a **Table** from others.
pub trait Id: PartialEq + for<'de> Deserialize<'de> + Serialize {}

/// Types that may be used as a **Key** into a **Table**.
pub trait Key: for<'de> Deserialize<'de> + Serialize {}

/// Types that may be used as a **Value** within a **Table**.
pub trait Value: for<'de> Deserialize<'de> + Serialize {}

/// **Read-only** access to a **Table** within a **sled::Tree**.
#[derive(Debug)]
pub struct Reader<'a, T> {
    tree: &'a sled::Tree,
    _table: PhantomData<T>,
}

/// Read and write access to a **Table** within a **sled::Tree**.
#[derive(Debug)]
pub struct Writer<'a, T> {
    reader: Reader<'a, T>,
}

/// An iterator yielding key/value pairs from a table of type `T`.
pub struct Iter<'a, T> {
    id_len: usize,
    iter: sled::Iter<'a>,
    _table: PhantomData<T>,
}

/// The possible errors that might occur while reading/writing a **Table** within a **sled::Tree**.
#[derive(Debug)]
pub enum Error {
    Sled(sled::Error<()>),
    Bincode(bincode::Error),
    Bytekey(bytekey::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

// Implementations

impl<'a, T> Reader<'a, T>
where
    T: Table,
{
    /// Retrieve a value from the **Tree** if it exists.
    pub fn get(&self, key: &T::Key) -> Result<Option<T::Value>> {
        let key_bytes = write_key::<T>(key)?;
        let maybe_value_bytes = self.tree.get(&key_bytes)?;
        match maybe_value_bytes {
            None => Ok(None),
            Some(value_bytes) => {
                let value = bincode::deserialize(&value_bytes)?;
                Ok(Some(value))
            },
        }
    }

    /// Iterate over all key value pairs in the table.
    pub fn iter(&self) -> Result<Iter<'a, T>> {
        let start_key_bytes: Vec<u8> = bytekey::serialize(&T::ID)?;
        let id_len = start_key_bytes.len();
        let iter = self.tree.scan(&start_key_bytes);
        let _table = PhantomData;
        Ok(Iter { id_len, iter, _table })
    }

    /// Iterate over tuples of keys and values, starting at the provided key.
    pub fn scan(&self, key: &T::Key) -> Result<Iter<'a, T>> {
        let id_len = bytekey::serialize(&T::ID)?.len();
        let key_bytes = write_key::<T>(key)?;
        let iter = self.tree.scan(&key_bytes);
        let _table = PhantomData;
        Ok(Iter { id_len, iter, _table })
    }

    /// Return the minimum entry within the table.
    ///
    /// This is similar to using the `iter().next()` method.
    pub fn min(&self) -> Result<Option<(T::Key, T::Value)>> {
        match self.iter()?.next() {
            None => Ok(None),
            Some(Err(err)) => Err(err),
            Some(Ok(kv)) => Ok(Some(kv)),
        }
    }

    /// Return the entry that is the successor of the given key.
    ///
    /// Returns `None` if no such key exists.
    ///
    /// This is similar to using the `scan(key).next()` method, but is non-inclusive of the given
    /// key.
    pub fn succ(&self, key: &T::Key) -> Result<Option<(T::Key, T::Value)>>
    where
        T::Key: PartialEq,
    {
        let mut iter = self.scan(key)?;
        loop {
            match iter.next() {
                None => return Ok(None),
                Some(Err(err)) => return Err(err),
                Some(Ok((k, v))) => match k == *key {
                    true => continue,
                    false => return Ok(Some((k, v))),
                }
            }
        }
    }

    /// Return the entry that is equal to or the successor of the given key.
    ///
    /// Returns `None` if no such key exists.
    ///
    /// This is similar to using the `scan(key).next()` method.
    pub fn succ_incl(&self, key: &T::Key) -> Result<Option<(T::Key, T::Value)>> {
        match self.scan(key)?.next() {
            None => Ok(None),
            Some(Err(err)) => Err(err),
            Some(Ok(kv)) => Ok(Some(kv)),
        }
    }
}

impl<'a, T> Reader<'a, T>
where
    T: Table,
    T::Key: UnsignedBinarySearchKey,
{
    /// Find and return the maximum entry within the table.
    ///
    /// This produces the same result as `iter().last()` but much more efficiently.
    pub fn max(&self) -> Result<Option<(T::Key, T::Value)>> {
        unsigned_binary_search::find_max(self)
    }

    /// Find and return the entry that is equal to or precedes the given key.
    ///
    /// Returns `None` if no such key exists.
    pub fn pred_incl(&self, key: &T::Key) -> Result<Option<(T::Key, T::Value)>> {
        unsigned_binary_search::find_pred(self, key, true)
    }

    /// Find and return the entry that precedes the given key.
    ///
    /// Returns `None` if no such key exists.
    pub fn pred(&self, key: &T::Key) -> Result<Option<(T::Key, T::Value)>> {
        unsigned_binary_search::find_pred(self, key, false)
    }
}

impl<'a, T> Writer<'a, T>
where
    T: Table,
{
    /// Set the given **key** to a new **value**.
    pub fn set(&self, key: &T::Key, value: &T::Value) -> Result<()> {
        let key_bytes = write_key::<T>(key)?;
        let value_bytes = bincode::serialize(value)?;
        self.tree.set(key_bytes, value_bytes)?;
        Ok(())
    }

    /// Remove a value from the **Tree** if it exists.
    pub fn del(&self, key: &T::Key) -> Result<Option<T::Value>> {
        let key_bytes = write_key::<T>(key)?;
        let maybe_value_bytes = self.tree.del(&key_bytes)?;
        match maybe_value_bytes {
            None => Ok(None),
            Some(value_bytes) => {
                let value = bincode::deserialize(&value_bytes)?;
                Ok(Some(value))
            },
        }
    }
}

// Trait implementations.

impl<T> Id for T where T: PartialEq + for<'de> Deserialize<'de> + Serialize {}

impl<T> Key for T where T: for<'de> Deserialize<'de> + Serialize {}

impl<T> Value for T where T: for<'de> Deserialize<'de> + Serialize {}

impl<'a, T> From<&'a sled::Tree> for Reader<'a, T> {
    fn from(tree: &'a sled::Tree) -> Self {
        let _table = PhantomData;
        Reader { tree, _table }
    }
}

impl<'a, T> From<&'a sled::Tree> for Writer<'a, T> {
    fn from(tree: &'a sled::Tree) -> Self {
        let reader = tree.into();
        Writer { reader }
    }
}

impl<'a, T> From<Writer<'a, T>> for Reader<'a, T> {
    fn from(writer: Writer<'a, T>) -> Self {
        writer.reader
    }
}

impl<'a, T> Clone for Reader<'a, T> {
    fn clone(&self) -> Self {
        let tree = self.tree;
        let _table = PhantomData;
        Reader { tree, _table }
    }
}

impl<'a, T> Clone for Writer<'a, T> {
    fn clone(&self) -> Self {
        let reader = self.reader.clone();
        Writer { reader }
    }
}

impl<'a, T> ops::Deref for Writer<'a, T> {
    type Target = Reader<'a, T>;
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Table,
{
    type Item = Result<(T::Key, T::Value)>;
    fn next(&mut self) -> Option<Self::Item> {
        let (id_key_bytes, value_bytes) = match self.iter.next() {
            None => return None,
            Some(Err(err)) => return Some(Err(err.into())),
            Some(Ok(tuple)) => tuple,
        };
        let id_bytes = &id_key_bytes[..self.id_len];
        let key_bytes = &id_key_bytes[self.id_len..];
        let id = match bytekey::deserialize(id_bytes) {
            Err(err) => return Some(Err(err.into())),
            Ok(id) => id,
        };
        if T::ID != id {
            return None;
        }
        let key = match bytekey::deserialize(key_bytes) {
            Err(err) => return Some(Err(err.into())),
            Ok(key) => key,
        };
        let value = match bincode::deserialize(&value_bytes) {
            Err(err) => return Some(Err(err.into())),
            Ok(value) => value,
        };
        Some(Ok((key, value)))
    }
}

/// Write a key for table `T` to bytes.
///
/// This simply pre-pends the serialized `key` with a serialised instance of the table `ID`.
pub fn write_key<T: Table>(key: &T::Key) -> bytekey::Result<Vec<u8>> {
    let mut key_bytes = vec![];
    bytekey::serialize_into(&mut key_bytes, &T::ID)?;
    bytekey::serialize_into(&mut key_bytes, key)?;
    Ok(key_bytes)
}

// Error implementations.

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Sled(ref err) => err.description(),
            Error::Bincode(ref err) => err.description(),
            Error::Bytekey(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self {
            Error::Sled(ref err) => Some(err),
            Error::Bincode(ref err) => Some(err),
            Error::Bytekey(ref err) => Some(err),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

impl From<sled::Error<()>> for Error {
    fn from(e: sled::Error<()>) -> Self {
        Error::Sled(e)
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::Bincode(e)
    }
}

impl From<bytekey::Error> for Error {
    fn from(e: bytekey::Error) -> Self {
        Error::Bytekey(e)
    }
}

impl From<bytekey::ser::Error> for Error {
    fn from(e: bytekey::ser::Error) -> Self {
        Error::Bytekey(e.into())
    }
}

impl From<bytekey::de::Error> for Error {
    fn from(e: bytekey::de::Error) -> Self {
        Error::Bytekey(e.into())
    }
}
