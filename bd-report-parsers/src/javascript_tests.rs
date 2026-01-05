// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use super::*;
use itertools::Itertools;

#[test]
fn parse_android_react_native_stack() {
  let stack = "Error: test\nat \
               render(/home/username/sample-workspace/sampleapp.collect.react/src/components/\
               GpsMonitorScene.js:78:24)\nat \
               _renderValidatedComponentWithoutOwnerOrContext(/home/username/sample-workspace/\
               sampleapp.collect.react/node_modules/react-native/Libraries/Renderer/src/renderers/\
               shared/stack/reconciler/ReactCompositeComponent.js:1050:29)";
  let frames = parse_javascript_stack_trace(stack, None).collect_vec();
  assert_eq!(frames.len(), 2);
  assert_eq!(frames[0].function_name, "render");
  assert_eq!(
    frames[0].file_path,
    "/home/username/sample-workspace/sampleapp.collect.react/src/components/GpsMonitorScene.js"
  );
  assert_eq!(frames[0].line, 78);
  assert_eq!(frames[0].column, 24);
  assert!(!frames[0].in_app);
  assert_eq!(
    frames[1].function_name,
    "_renderValidatedComponentWithoutOwnerOrContext"
  );
  assert_eq!(
    frames[1].file_path,
    "/home/username/sample-workspace/sampleapp.collect.react/node_modules/react-native/Libraries/\
     Renderer/src/renderers/shared/stack/reconciler/ReactCompositeComponent.js"
  );
  assert!(!frames[1].in_app);
}

#[test]
fn parse_ios_react_native_1_stack() {
  let stack = "_exampleFunction@/home/test/project/App.js:125:13\n_depRunCallbacks@/home/test/\
               project/node_modules/dep/index.js:77:45\ntryCallTwo@/home/test/project/\
               node_modules/react-native/node_modules/promise/lib/core.js:45:5";
  let frames = parse_javascript_stack_trace(stack, None).collect_vec();
  assert_eq!(frames.len(), 3);
  assert_eq!(frames[0].function_name, "_exampleFunction");
  assert_eq!(frames[0].file_path, "/home/test/project/App.js");
  assert_eq!(frames[0].line, 125);
  assert_eq!(frames[0].column, 13);
  assert!(!frames[0].in_app);
  assert!(!frames[1].in_app);
  assert!(!frames[2].in_app);
}

#[test]
fn parse_android_react_native_prod_stack() {
  let stack = "value@index.android.bundle:12:1917\nonPress@index.android.bundle:12:2336\\
                 ntouchableHandlePress@index.android.bundle:258:1497\n[native code]";
  let frames = parse_javascript_stack_trace(stack, None).collect_vec();
  assert_eq!(frames.len(), 4);
  assert_eq!(frames[0].function_name, "value");
  assert_eq!(frames[0].file_path, "index.android.bundle");
  assert_eq!(frames[0].line, 12);
  assert_eq!(frames[0].column, 1917);
  assert_eq!(
    frames[0].bundle_path,
    Some("index.android.bundle".to_string())
  );
  assert!(!frames[0].in_app);
  assert_eq!(frames[1].function_name, "onPress");
  assert_eq!(frames[1].file_path, "index.android.bundle");
  assert_eq!(
    frames[1].bundle_path,
    Some("index.android.bundle".to_string())
  );
  assert!(!frames[1].in_app);
  assert_eq!(frames[3].function_name, "[native code]");
  assert_eq!(frames[3].file_path, "[native code]");
  assert_eq!(frames[3].line, 0);
  assert_eq!(frames[3].column, 0);
  assert!(!frames[3].in_app);
}

#[test]
fn parse_release_build_with_address_at_prefix() {
  let stack = "triggerGlobalJsError@address at \
               index.android.bundle:1:726416\n_performTransitionSideEffects@address at \
               index.android.bundle:1:460430";
  let frames = parse_javascript_stack_trace(stack, None).collect_vec();
  assert_eq!(frames.len(), 2);
  assert_eq!(frames[0].function_name, "triggerGlobalJsError");
  assert_eq!(frames[0].file_path, "index.android.bundle");
  assert_eq!(frames[0].line, 1);
  assert_eq!(frames[0].column, 726_416);
  assert_eq!(
    frames[0].bundle_path,
    Some("index.android.bundle".to_string())
  );
  assert!(!frames[0].in_app);
  assert_eq!(frames[1].function_name, "_performTransitionSideEffects");
  assert_eq!(frames[1].file_path, "index.android.bundle");
  assert_eq!(frames[1].line, 1);
  assert_eq!(frames[1].column, 460_430);
  assert_eq!(
    frames[1].bundle_path,
    Some("index.android.bundle".to_string())
  );
  assert!(!frames[1].in_app);
}

#[test]
fn parse_release_build_at_format_with_address_at() {
  let stack = "Error: Triggered Global JS Error - Intentional for testing\n    at \
               triggerGlobalJsError (address at index.android.bundle:1:726416)\n    at \
               _performTransitionSideEffects (address at index.android.bundle:1:460430)\n    at \
               forEach (native)";
  let frames = parse_javascript_stack_trace(stack, None).collect_vec();
  assert_eq!(frames.len(), 3);
  assert_eq!(frames[0].function_name, "triggerGlobalJsError");
  assert_eq!(frames[0].file_path, "index.android.bundle");
  assert_eq!(frames[0].line, 1);
  assert_eq!(frames[0].column, 726_416);
  assert_eq!(
    frames[0].bundle_path,
    Some("index.android.bundle".to_string())
  );
  assert!(!frames[0].in_app);
  assert_eq!(frames[1].function_name, "_performTransitionSideEffects");
  assert_eq!(frames[1].file_path, "index.android.bundle");
  assert_eq!(frames[1].line, 1);
  assert_eq!(frames[1].column, 460_430);
  assert_eq!(
    frames[1].bundle_path,
    Some("index.android.bundle".to_string())
  );
  assert!(!frames[1].in_app);
  assert_eq!(frames[2].function_name, "forEach");
  assert_eq!(frames[2].file_path, "native");
  assert_eq!(frames[2].line, 0);
  assert_eq!(frames[2].column, 0);
  assert!(!frames[2].in_app);
}

#[test]
fn parse_ios_jsbundle_release_build() {
  let stack = concat!(
    "triggerGlobalJsError@main.jsbundle:1:717458\n",
    "onPress@main.jsbundle:1:720868\n",
    "_performTransitionSideEffects@main.jsbundle:1:456325\n",
    "[native code]"
  );
  let frames =
    parse_javascript_stack_trace(stack, Some("5aa81d1c6931a5db1800b6f5ee411e9e")).collect_vec();
  assert_eq!(frames.len(), 4);
  assert_eq!(frames[0].function_name, "triggerGlobalJsError");
  assert_eq!(frames[0].file_path, "main.jsbundle");
  assert_eq!(frames[0].line, 1);
  assert_eq!(frames[0].column, 717_458);
  assert_eq!(frames[0].bundle_path, Some("main.jsbundle".to_string()));
  assert_eq!(
    frames[0].image_id,
    Some("5aa81d1c6931a5db1800b6f5ee411e9e".to_string())
  );
  assert!(!frames[0].in_app);
  assert_eq!(frames[1].function_name, "onPress");
  assert_eq!(frames[1].file_path, "main.jsbundle");
  assert_eq!(frames[1].bundle_path, Some("main.jsbundle".to_string()));
  assert!(!frames[1].in_app);
  assert_eq!(frames[3].function_name, "[native code]");
  assert_eq!(frames[3].file_path, "[native code]");
  assert!(!frames[3].in_app);
}
