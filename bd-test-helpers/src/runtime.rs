// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_proto::protos::client::api::RuntimeUpdate;
use bd_proto::protos::client::runtime::Runtime;
use bd_proto::protos::client::runtime::runtime::{Value, value};

/// A simple representation of a runtime value. This is used to provide better ergonomics than the
/// protobuf enums.
#[derive(Clone)]
pub enum ValueKind {
  Bool(bool),
  Int(u32),
  String(String),
}

impl From<ValueKind> for Value {
  fn from(v: ValueKind) -> Self {
    Self {
      type_: Some(match v {
        ValueKind::Bool(b) => value::Type::BoolValue(b),
        ValueKind::Int(i) => value::Type::UintValue(i),
        ValueKind::String(s) => value::Type::StringValue(s),
      }),
      ..Default::default()
    }
  }
}

/// Helper for building a Runtime protobuf object.
#[must_use]
pub fn make_proto(entries: Vec<(&str, ValueKind)>) -> Runtime {
  Runtime {
    values: entries
      .into_iter()
      .map(|e| (e.0.to_string(), e.1.into()))
      .collect(),
    ..Default::default()
  }
}

#[must_use]
pub fn make_update(entries: Vec<(&str, ValueKind)>, version_nonce: String) -> RuntimeUpdate {
  RuntimeUpdate {
    version_nonce,
    runtime: Some(make_proto(entries)).into(),
    ..Default::default()
  }
}

#[must_use]
pub fn make_simple_update(entries: Vec<(&str, ValueKind)>) -> RuntimeUpdate {
  RuntimeUpdate {
    version_nonce: "simple".to_string(),
    runtime: Some(make_proto(entries)).into(),
    ..Default::default()
  }
}
