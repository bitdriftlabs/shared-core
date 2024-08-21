// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use axum::extract::Query;
use axum::routing::post;
use axum::Router;
use std::collections::HashMap;
use std::ffi::CString;

fn result_to_string(result: tikv_jemalloc_ctl::Result<()>) -> String {
  result.map_or_else(|e| format!("error: {e}"), |()| "OK".to_string())
}

async fn profile_enable() -> String {
  unsafe { result_to_string(tikv_jemalloc_ctl::raw::write(b"prof.active\0", true)) }
}

async fn profile_disable() -> String {
  unsafe { result_to_string(tikv_jemalloc_ctl::raw::write(b"prof.active\0", false)) }
}

async fn profile_dump(Query(mut params): Query<HashMap<String, String>>) -> String {
  let output_file = params
    .remove("file")
    .unwrap_or_else(|| "/tmp/profile.heap".to_string());
  let output_file = CString::new(output_file).unwrap();

  unsafe {
    result_to_string(tikv_jemalloc_ctl::raw::write_str(
      b"prof.dump\0",
      std::mem::transmute::<&[u8], &[u8]>(output_file.as_bytes_with_nul()),
    ))
  }
}

pub fn new_router() -> Router {
  Router::new()
    .route("/profile_enable", post(profile_enable))
    .route("/profile_disable", post(profile_disable))
    .route("/profile_dump", post(profile_dump))
}
