// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[repr(u8)]
pub enum TypeCode {
  P100 = 0x64,
  LongString = 0x68,
  LongNumber = 0x69,
  Float16 = 0x6a,
  Float32 = 0x6b,
  Float64 = 0x6c,
  Null = 0x6d,
  False = 0x6e,
  True = 0x6f,
  Unsigned = 0x70,
  UnsignedEnd = 0x77,
  Signed = 0x78,
  SignedEnd = 0x7f,
  String = 0x80,
  StringEnd = 0x8f,
  ArrayStart = 0x99,
  MapStart = 0x9a,
  ContainerEnd = 0x9b,
  N100 = 0x9c,
}
