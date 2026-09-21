// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#[path = "../proto_codegen.rs"]
mod proto_codegen;

fn main() {
  proto_codegen::generate_protos();
}
