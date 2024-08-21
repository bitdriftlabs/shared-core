// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![cfg(target_family = "unix")]

//! Functionality for dynamically (re)loading a configuration file and providing
//! configuration data to a running application in a type safe way, geared
//! towards use with Kubernetes `ConfigMap` resources.
//!
//! Generic configuration file load is provided via the [`loader`] module. An
//! example looks like:
//!
//! Mount a `ConfigMap` that contains `test.yaml` in `/config_map/test`.
//! `test.yaml` contains:
//!
//! ```yaml
//! foo: 23
//! bar: hello
//! ```
//!
//! The following code can be used to load the configuration file. Further
//! `ConfigMap` updates will reload it.
//!
//! ```no_run
//! use bd_runtime_config::loader::*;
//! use std::path::Path;
//! use std::sync::Arc;
//!
//! #[derive(serde::Deserialize, Debug)]
//! struct TestData {
//!   foo: u64,
//!   bar: String,
//! }
//!
//! let collector = bd_server_stats::stats::Collector::default();
//! let scope = collector.scope("loader");
//!
//! let loader = WatchedFileLoader::new_loader(
//!   "/config_map/test",
//!   "/config_map/test/test.yaml",
//!   |test_data: Option<TestData>| -> ConfigPtr<TestData> { test_data.map(Arc::new) },
//!   Stats::new(&scope),
//! )
//! .unwrap();
//!
//! // Add appropriate error checking for lack of configuration file as `unwrap()` may fail.
//! assert_eq!(loader.snapshot_watch().borrow().as_ref().unwrap().foo, 23);
//! assert_eq!(
//!   loader.snapshot_watch().borrow().as_ref().unwrap().bar,
//!   "hello"
//! );
//! ```
//!
//! For feature flags (a map of string keys to bool/int/string values), a more
//! strongly typed layer is available on top via the [`feature_flags`] module.
//! An example of using this module:
//!
//! Mount a `ConfigMap` that contains `test.yaml` in `/config_map/test`.
//! `test.yaml` contains:
//!
//! ```yaml
//! values:
//!   foo: 1
//!   bar: hello
//!   baz: true
//! ```
//!
//! The following code can be used to load the feature flags. Further
//! `ConfigMap` updates will reload it.
//!
//! ```no_run
//! use bd_runtime_config::feature_flags::*;
//! use bd_runtime_config::loader::*;
//! use std::path::Path;
//!
//! let collector = bd_server_stats::stats::Collector::default();
//! let scope = collector.scope("loader");
//!
//! let loader = new_memory_feature_flags_loader(
//!   Path::new("/config_map/test"),
//!   Path::new("/config_map/test/test.yaml"),
//!   Stats::new(&scope),
//! )
//! .unwrap();
//!
//! // Note that feature flags are always present so unwrap() can be used safely.
//! assert!(loader
//!   .snapshot_watch()
//!   .borrow()
//!   .as_ref()
//!   .unwrap()
//!   .get_bool("baz", false));
//! assert_eq!(
//!   loader
//!     .snapshot_watch()
//!     .borrow()
//!     .as_ref()
//!     .unwrap()
//!     .get_integer("foo", 0),
//!   1
//! );
//! assert_eq!(
//!   loader
//!     .snapshot_watch()
//!     .borrow()
//!     .as_ref()
//!     .unwrap()
//!     .get_string("bar", "world"),
//!   "hello"
//! );
//! ```

/// Configuration file watcher and loader.
pub mod loader;

/// Specific implementation of [`loader`] geared towards feature flags.
pub mod feature_flags;

#[cfg(test)]
mod test;
