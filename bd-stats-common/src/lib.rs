// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use sketches_rust::DDSketch;
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

#[must_use]
pub fn make_client_sketch() -> DDSketch {
  // For this sketch, the index parameters must be consistent in order to perform merges.
  // However, the maximum number of buckets do not have to be the same. This means that
  // we can use less buckets on the client and more on the server if we like as long as
  // the other index parameters are the same. For now using the logarithmic collapsing
  // variant seems the best. 0.02 relative accuracy is arbitrary as well as a max of 128
  // buckets. We can tune this later as we have more time to devote to understanding the
  // math.
  DDSketch::logarithmic_collapsing_lowest_dense(0.02, 128).unwrap()
}

//
// MetricType
//

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, PartialOrd, Ord)]
pub enum MetricType {
  Counter,
  Histogram,
}

//
// NameType
//

#[derive(Eq, Hash, PartialEq, Clone)]
pub enum NameType {
  Global(String),
  ActionId(String),
}

impl NameType {
  #[must_use]
  pub fn as_str(&self) -> &str {
    match self {
      Self::Global(name) | Self::ActionId(name) => name,
    }
  }

  #[must_use]
  pub fn into_string(self) -> String {
    match self {
      Self::Global(name) | Self::ActionId(name) => name,
    }
  }
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
  pub const fn new(name: String, labels: BTreeMap<String, String>) -> Self {
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
