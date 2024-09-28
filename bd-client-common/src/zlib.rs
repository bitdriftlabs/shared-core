// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

// zlib has 10 compression levels (0-9). We use level 5 as this is what Apple says about
// this particular compression level: "This compression level provides a good balance
// between compression speed and compression ratio.".
// Source: https://developer.apple.com/documentation/compression/algorithm/zlib
pub const DEFAULT_MOBILE_ZLIB_COMPRESSION_LEVEL: u32 = 5;
