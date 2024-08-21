// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use bd_log_primitives::LogField;
use bd_matcher::FieldProvider;

//
// FieldsContainer
//

pub struct FieldsContainer<'a> {
  pub fields: &'a Vec<LogField>,
}

impl FieldProvider for FieldsContainer<'_> {
  fn field_value(&self, field_key: &str) -> Option<&str> {
    self
      .fields
      .iter()
      .find(|field| field.key == field_key)
      .and_then(|field| field.value.as_str())
  }
}
