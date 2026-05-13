// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

mod add_field;
mod set_grouping_key;
mod set_significant_frame;

use crate::functions::add_field::AddField;
use crate::functions::set_grouping_key::SetGroupingKey;
use crate::functions::set_significant_frame::SetSignificantFrame;
use vrl::prelude::Function;


pub fn all() -> Vec<Box<dyn Function>> {
  vec![
    Box::new(AddField),
    Box::new(SetGroupingKey),
    Box::new(SetSignificantFrame),
  ]
}
