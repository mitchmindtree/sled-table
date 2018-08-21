//! Items related to performing a binary search over an unsigned integer range.

use {Reader, Result, Table};
use std::{self, ops};

/// A trait implemented for unsigned integer types that may be used in binary search.
pub trait UnsignedInteger: Sized
    + Copy
    + PartialEq
    + PartialOrd
    + ops::Add<Output = Self>
    + ops::Sub<Output = Self>
    + ops::Div<Output = Self>
{
    const MAX: Self;
    const ONE: Self;
    const ZERO: Self;
}

/// Binary search utility for finding the greatest key in a table.
///
/// Note: This will probably be removed in favour of some better, more generic option, or if we ever
/// get a response to this: https://github.com/spacejam/sled/issues/330
pub trait UnsignedBinarySearchKey: PartialEq + PartialOrd {
    /// The unsigned integer representation of the key.
    type UnsignedInteger: UnsignedInteger;
    /// Create a key from its unsigned integer representation.
    fn from_unsigned_integer(u: Self::UnsignedInteger) -> Self;
}

impl UnsignedInteger for u8 {
    const MAX: Self = std::u8::MAX;
    const ONE: Self = 1;
    const ZERO: Self = 0;
}

impl UnsignedInteger for u16 {
    const MAX: Self = std::u16::MAX;
    const ONE: Self = 1;
    const ZERO: Self = 0;
}

impl UnsignedInteger for u32 {
    const MAX: Self = std::u32::MAX;
    const ONE: Self = 1;
    const ZERO: Self = 0;
}

impl UnsignedInteger for u64 {
    const MAX: Self = std::u64::MAX;
    const ONE: Self = 1;
    const ZERO: Self = 0;
}

impl UnsignedInteger for usize {
    const MAX: Self = std::usize::MAX;
    const ONE: Self = 1;
    const ZERO: Self = 0;
}

impl UnsignedBinarySearchKey for u8 {
    type UnsignedInteger = u8;
    fn from_unsigned_integer(u: Self::UnsignedInteger) -> Self {
        u
    }
}

impl UnsignedBinarySearchKey for u16 {
    type UnsignedInteger = u16;
    fn from_unsigned_integer(u: Self::UnsignedInteger) -> Self {
        u
    }
}

impl UnsignedBinarySearchKey for u32 {
    type UnsignedInteger = u32;
    fn from_unsigned_integer(u: Self::UnsignedInteger) -> Self {
        u
    }
}

impl UnsignedBinarySearchKey for u64 {
    type UnsignedInteger = u64;
    fn from_unsigned_integer(u: Self::UnsignedInteger) -> Self {
        u
    }
}

impl UnsignedBinarySearchKey for usize {
    type UnsignedInteger = usize;
    fn from_unsigned_integer(u: Self::UnsignedInteger) -> Self {
        u
    }
}

/// Find the maximum entry that precedes the given key within the given `Table` using a binary
/// search over the key's associated unsigned integer range.
pub fn find_pred<T>(
    table: &Reader<T>,
    key: &T::Key,
    inclusive: bool,
) -> Result<Option<(T::Key, T::Value)>>
where
    T: Table,
    T::Key: UnsignedBinarySearchKey,
{
    let max = <<T::Key as UnsignedBinarySearchKey>::UnsignedInteger as UnsignedInteger>::MAX;
    let one = <<T::Key as UnsignedBinarySearchKey>::UnsignedInteger as UnsignedInteger>::ONE;
    let zero = <<T::Key as UnsignedBinarySearchKey>::UnsignedInteger as UnsignedInteger>::ZERO;
    let two = one + one;
    let mut attempt = max / two + one;
    let mut step = attempt;
    let mut greatest = None;
    while step != zero {
        step = step / two;
        let attempt_key: T::Key = UnsignedBinarySearchKey::from_unsigned_integer(attempt);
        match table.scan(&attempt_key)?.next() {
            // If there's an error, return it.
            Some(Err(err)) => return Err(err),
            // If it's greater than or equal to the attempt, assign and search upwards.
            Some(Ok((k, v))) => match if inclusive { k <= *key } else { k < *key } {
                true => {
                    greatest = Some((k, v));
                    attempt = attempt + step;
                }
                false => attempt = attempt - step,
            },
            // If there's no keys, search downwards.
            None => attempt = attempt - step,
        }
    }
    if greatest.is_none() {
        let k = UnsignedBinarySearchKey::from_unsigned_integer(zero);
        if let Some(v) = table.get(&k)? {
            greatest = Some((k, v));
        }
    }
    Ok(greatest)
}

/// Find the maximum entry within the given `Table` using a binary search over the key's associated
/// unsigned integer range.
pub fn find_max<T>(table: &Reader<T>) -> Result<Option<(T::Key, T::Value)>>
where
    T: Table,
    T::Key: UnsignedBinarySearchKey,
{
    let max = <<T::Key as UnsignedBinarySearchKey>::UnsignedInteger as UnsignedInteger>::MAX;
    let one = <<T::Key as UnsignedBinarySearchKey>::UnsignedInteger as UnsignedInteger>::ONE;
    let zero = <<T::Key as UnsignedBinarySearchKey>::UnsignedInteger as UnsignedInteger>::ZERO;
    let two = one + one;
    let mut attempt = max / two + one;
    let mut step = attempt;
    let mut greatest = None;
    while step != zero {
        step = step / two;
        let attempt_key: T::Key = UnsignedBinarySearchKey::from_unsigned_integer(attempt);
        match table.scan(&attempt_key)?.next() {
            // If there's an error, return it.
            Some(Err(err)) => return Err(err),
            // If it's greater than or equal to the attempt, assign and search upwards.
            Some(Ok((k, v))) => {
                greatest = Some((k, v));
                attempt = attempt + step;
            },
            // If there's no keys, search downwards.
            None => attempt = attempt - step,
        }
    }
    if greatest.is_none() {
        let k = UnsignedBinarySearchKey::from_unsigned_integer(zero);
        if let Some(v) = table.get(&k)? {
            greatest = Some((k, v));
        }
    }
    Ok(greatest)
}
