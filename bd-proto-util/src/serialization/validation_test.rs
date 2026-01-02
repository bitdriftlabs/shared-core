// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![allow(clippy::unwrap_used)]

use super::{CanonicalType, ValidationResult, validate_field_type};
use protobuf::reflect::{RuntimeFieldType, RuntimeType};

// Test helpers to create RuntimeFieldType values
fn singular(rt: RuntimeType) -> RuntimeFieldType {
  RuntimeFieldType::Singular(rt)
}

fn repeated(rt: RuntimeType) -> RuntimeFieldType {
  RuntimeFieldType::Repeated(rt)
}

fn map(k: RuntimeType, v: RuntimeType) -> RuntimeFieldType {
  RuntimeFieldType::Map(k, v)
}

// ============================================================================
// Positive tests - types that should match
// ============================================================================

#[test]
fn string_matches_string() {
  let result = validate_field_type(&CanonicalType::String, &singular(RuntimeType::String));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn bytes_matches_vec_u8() {
  let result = validate_field_type(&CanonicalType::Bytes, &singular(RuntimeType::VecU8));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn bool_matches_bool() {
  let result = validate_field_type(&CanonicalType::Bool, &singular(RuntimeType::Bool));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn i32_matches_i32() {
  let result = validate_field_type(&CanonicalType::I32, &singular(RuntimeType::I32));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn i64_matches_i64() {
  let result = validate_field_type(&CanonicalType::I64, &singular(RuntimeType::I64));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn u32_matches_u32() {
  let result = validate_field_type(&CanonicalType::U32, &singular(RuntimeType::U32));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn u64_matches_u64() {
  let result = validate_field_type(&CanonicalType::U64, &singular(RuntimeType::U64));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn f32_matches_f32() {
  let result = validate_field_type(&CanonicalType::F32, &singular(RuntimeType::F32));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn f64_matches_f64() {
  let result = validate_field_type(&CanonicalType::F64, &singular(RuntimeType::F64));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn optional_string_matches_singular_string() {
  // Optional wrapper is unwrapped before comparison
  let optional = CanonicalType::Optional(Box::new(CanonicalType::String));
  let result = validate_field_type(&optional, &singular(RuntimeType::String));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn repeated_string_matches_repeated_string() {
  let repeated_type = CanonicalType::Repeated(Box::new(CanonicalType::String));
  let result = validate_field_type(&repeated_type, &repeated(RuntimeType::String));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn repeated_i32_matches_repeated_i32() {
  let repeated_type = CanonicalType::Repeated(Box::new(CanonicalType::I32));
  let result = validate_field_type(&repeated_type, &repeated(RuntimeType::I32));
  assert_eq!(result, ValidationResult::Ok);
}

#[test]
fn map_matches_map() {
  let map_type = CanonicalType::Map(
    Box::new(CanonicalType::String),
    Box::new(CanonicalType::I32),
  );
  let result = validate_field_type(&map_type, &map(RuntimeType::String, RuntimeType::I32));
  assert_eq!(result, ValidationResult::Ok);
}

// ============================================================================
// Negative tests - types that should NOT match
// ============================================================================

#[test]
fn string_does_not_match_i32() {
  let result = validate_field_type(&CanonicalType::String, &singular(RuntimeType::I32));
  assert!(matches!(result, ValidationResult::TypeMismatch { .. }));

  if let ValidationResult::TypeMismatch { expected, actual } = result {
    assert_eq!(expected, "String");
    assert!(actual.contains("I32"));
  }
}

#[test]
fn i32_does_not_match_string() {
  let result = validate_field_type(&CanonicalType::I32, &singular(RuntimeType::String));
  assert!(matches!(result, ValidationResult::TypeMismatch { .. }));
}

#[test]
fn i32_does_not_match_i64() {
  let result = validate_field_type(&CanonicalType::I32, &singular(RuntimeType::I64));
  assert!(matches!(result, ValidationResult::TypeMismatch { .. }));
}

#[test]
fn u32_does_not_match_i32() {
  let result = validate_field_type(&CanonicalType::U32, &singular(RuntimeType::I32));
  assert!(matches!(result, ValidationResult::TypeMismatch { .. }));
}

#[test]
fn f32_does_not_match_f64() {
  let result = validate_field_type(&CanonicalType::F32, &singular(RuntimeType::F64));
  assert!(matches!(result, ValidationResult::TypeMismatch { .. }));
}

#[test]
fn singular_does_not_match_repeated() {
  let result = validate_field_type(&CanonicalType::String, &repeated(RuntimeType::String));
  assert!(matches!(result, ValidationResult::TypeMismatch { .. }));
}

#[test]
fn repeated_string_does_not_match_repeated_i32() {
  let repeated_type = CanonicalType::Repeated(Box::new(CanonicalType::String));
  let result = validate_field_type(&repeated_type, &repeated(RuntimeType::I32));
  assert!(matches!(result, ValidationResult::TypeMismatch { .. }));
}

#[test]
fn bytes_does_not_match_string() {
  let result = validate_field_type(&CanonicalType::Bytes, &singular(RuntimeType::String));
  assert!(matches!(result, ValidationResult::TypeMismatch { .. }));
}

#[test]
fn bool_does_not_match_i32() {
  let result = validate_field_type(&CanonicalType::Bool, &singular(RuntimeType::I32));
  assert!(matches!(result, ValidationResult::TypeMismatch { .. }));
}

// ============================================================================
// into_result tests
// ============================================================================

#[test]
fn into_result_ok() {
  let result = ValidationResult::Ok;
  assert!(result.into_result("test_field").is_ok());
}

#[test]
fn into_result_error_includes_field_name() {
  let result = ValidationResult::TypeMismatch {
    expected: "String",
    actual: "I32".to_string(),
  };
  let err = result.into_result("my_field").unwrap_err();
  assert!(err.contains("my_field"));
  assert!(err.contains("String"));
  assert!(err.contains("I32"));
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn nested_optional_unwraps_fully() {
  // Option<Option<String>> should still match String
  let deeply_nested = CanonicalType::Optional(Box::new(CanonicalType::Optional(Box::new(
    CanonicalType::String,
  ))));
  let result = validate_field_type(&deeply_nested, &singular(RuntimeType::String));
  assert_eq!(result, ValidationResult::Ok);
}
