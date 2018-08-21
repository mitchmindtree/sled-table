# sled-table [![Build Status](https://travis-ci.org/mitchmindtree/sled-table.svg?branch=master)](https://travis-ci.org/mitchmindtree/sled-table)

Provides a typed table API around a `sled::Tree`.

Also provides `Timestamped` table abstraction (tables that can be searched by
either key or timestamp) and `Reversible` table abstraction (tables that can be
searched by either their key or their value.

Uses the `bytekey` crate for serialization of keys and the `bincode` crate for
serialization of values.

This was developed out of necessity for a private downstream project. If you
would like more details on how the crate works, please get in touch and I'll do
my best to provide more info. For now, the tests are your best hope of getting a
demonstration of usage (albeit raw and simple).

*Publication to crates.io is currently pending [danburkert/bytekey#4](https://github.com/danburkert/bytekey/pull/4)*.
