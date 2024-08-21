// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

#[macro_export]
macro_rules! labels {
  ( $( $ KEY : expr => $ VALUE : expr ),* $(,)? ) => {
    {
      use std::collections::BTreeMap;

      let mut lbs = BTreeMap::new();
      $(
        lbs.insert($KEY.into(), $VALUE.into());
      )*

      lbs
    }
  };
}

//
// Id
//

// Used as a key to de-dup global metrics.
// TODO(mattklein123): Now that we are using this for dynamic lookup of metrics on the client we
// should consider making this more efficient. We can consider bringing in some of the code we
// we have in pulse proxy for metrics lookup. Granted, that code relies on hashbrown APIs which
// we cannot use on the client for size reasons.
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct Id {
  pub name: String,
  pub labels: BTreeMap<String, String>,
}

impl Id {
  #[must_use]
  pub fn new(name: String, labels: BTreeMap<String, String>) -> Self {
    Self { name, labels }
  }
}

//
// Counter
//

pub trait Counter: Send + Sync + Debug {
  // Increment the counter by 1.
  fn inc(&self);

  // Increment the counter by the given value.
  fn inc_by(&self, value: u64);
}

pub type DynCounter = Arc<dyn Counter>;
