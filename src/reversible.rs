use sled;
use std::ops;
use {Result, Table};

/// An extension to the **Table** trait that allows for bi-directional conversions with some other
/// table.
pub trait Reversible: Table {
    /// The table used to perform the reverse conversion of this table.
    type ReverseTable: Table<Id = Self::Id, Key = Self::Value, Value = Self::Key>;
}

/// Read and write access to a reversible table within a `sled::Tree`.
#[derive(Debug)]
pub struct Writer<'a, T>
where
    T: Reversible,
{
    pub(crate) table: ::Writer<'a, T>,
    pub(crate) reverse_table: ::Writer<'a, T::ReverseTable>,
}

/// Read-only access to a reversible table within a `sled::Tree`.
#[derive(Debug)]
pub struct Reader<'a, T>
where
    T: Reversible,
{
    table: ::Reader<'a, T>,
    reverse_table: ::Reader<'a, T::ReverseTable>,
}

// Inherent implementations.

impl<'a, T> Writer<'a, T>
where
    T: Reversible,
{
    /// Set the given **key** to the new **value**.
    ///
    /// Also ensures that the inverse entry is added to **T::ReverseTable**.
    ///
    /// If either the key XOR value already exist, this method will `panic!` to ensure uniqueness
    /// between pairs.
    pub fn set(&self, key: &T::Key, value: &T::Value) -> Result<()> {
        assert_eq!(
            self.table.get(key).ok().and_then(|opt| opt).is_some(),
            self.reverse_table.get(value).ok().and_then(|opt| opt).is_some(),
        );
        self.table.set(key, value)?;
        self.reverse_table.set(value, key)?;
        Ok(())
    }

    /// Remove the entry for the given **key** from the table.
    ///
    /// Also removes the reverse entry from the reverse table.
    pub fn del(&self, key: &T::Key) -> Result<Option<T::Value>> {
        let maybe_value = self.table.del(key)?;
        if let Some(ref value) = maybe_value {
            self.reverse_table.del(value)?;
        }
        Ok(maybe_value)
    }

    /// Return the inverse of this table.
    pub fn inv(&self) -> Writer<'a, T::ReverseTable>
    where
        T::ReverseTable: Reversible<ReverseTable = T>,
    {
        let reverse_table = self.table.clone();
        let table = self.reverse_table.clone();
        Writer { table, reverse_table }
    }
}

impl<'a, T> Reader<'a, T>
where
    T: Reversible,
    T::ReverseTable: Reversible<ReverseTable = T>,
{
    /// Read-only acces to the inverse of this table, using `Value` as `Key` and vice versa.
    pub fn inv(&self) -> Reader<'a, T::ReverseTable> {
        let reverse_table = self.table.clone();
        let table = self.reverse_table.clone();
        Reader { table, reverse_table }
    }
}

// Trait implementations.

impl<'a, T> From<&'a sled::Tree> for Reader<'a, T>
where
    T: Reversible,
{
    fn from(tree: &'a sled::Tree) -> Self {
        let table = tree.into();
        let reverse_table = tree.into();
        Reader {
            table,
            reverse_table,
        }
    }
}

impl<'a, T> From<&'a sled::Tree> for Writer<'a, T>
where
    T: Reversible,
{
    fn from(tree: &'a sled::Tree) -> Self {
        let table = tree.into();
        let reverse_table = tree.into();
        Writer {
            table,
            reverse_table,
        }
    }
}

impl<'a, T> From<Writer<'a, T>> for Reader<'a, T>
where
    T: Reversible,
{
    fn from(w: Writer<'a, T>) -> Self {
        let table = w.table.clone().into();
        let reverse_table = w.reverse_table.clone().into();
        Reader { table, reverse_table }
    }
}

impl<'a, T> Clone for Reader<'a, T>
where
    T: Reversible,
{
    fn clone(&self) -> Self {
        let table = self.table.clone();
        let reverse_table = self.reverse_table.clone();
        Reader { table, reverse_table }
    }
}

impl<'a, T> Clone for Writer<'a, T>
where
    T: Reversible,
{
    fn clone(&self) -> Self {
        let table = self.table.clone();
        let reverse_table = self.reverse_table.clone();
        Writer { table, reverse_table }
    }
}

impl<'a, T> ops::Deref for Reader<'a, T>
where
    T: Reversible,
{
    type Target = ::Reader<'a, T>;
    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl<'a, T> ops::Deref for Writer<'a, T>
where
    T: Reversible,
{
    type Target = ::Reader<'a, T>;
    fn deref(&self) -> &Self::Target {
        &self.table
    }
}
