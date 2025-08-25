// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// Test code only.
#![allow(clippy::panic, clippy::unwrap_used)]

use crate::reporter::{Reporter, UnexpectedErrorHandler};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

//
// PanickingErrorReporter
//

#[derive(Default)]
pub struct PanickingErrorReporter;

impl PanickingErrorReporter {
  pub fn enable() {
    UnexpectedErrorHandler::set_reporter(Arc::new(Self));
  }
}

impl Reporter for PanickingErrorReporter {
  fn report(
    &self,
    message: &str,
    details: &Option<String>,
    fields: &HashMap<Cow<'_, str>, Cow<'_, str>>,
  ) {
    panic!("unexpected error: {message} {details:?} {fields:?}");
  }
}
