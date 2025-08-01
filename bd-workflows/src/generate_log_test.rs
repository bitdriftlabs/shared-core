// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use crate::generate_log::generate_log_action;
use crate::workflow::TraversalExtractions;
use bd_log_primitives::{FieldsRef, Log, LogFieldKey, LogFields, LogType, log_level};
use bd_proto::protos::workflow::workflow::workflow::action::ActionGenerateLog;
use bd_test_helpers::workflow::{TestFieldRef, TestFieldType, make_generate_log_action};
use pretty_assertions::assert_eq;
use time::OffsetDateTime;
use time::macros::datetime;
use uuid::Uuid;

//
// Helper
//

struct Helper {
  extractions: TraversalExtractions,
  captured_fields: LogFields,
  matching_fields: LogFields,
}

impl Helper {
  fn new() -> Self {
    let extractions = TraversalExtractions::default();
    let captured_fields = LogFields::default();
    let matching_fields = LogFields::default();
    Self {
      extractions,
      captured_fields,
      matching_fields,
    }
  }

  fn expect_log(
    &self,
    message: &str,
    fields: &[(&str, &str)],
    log_type: LogType,
    action: &ActionGenerateLog,
  ) {
    assert_eq!(
      Some(Log {
        log_level: log_level::DEBUG,
        log_type,
        message: message.into(),
        fields: fields
          .iter()
          .map(|(k, v)| ((*k).to_string().into(), (*v).into(),))
          .collect(),
        matching_fields: [("_generate_log_id".into(), "id".into(),),].into(),
        session_id: String::new(),
        occurred_at: OffsetDateTime::UNIX_EPOCH,
        capture_session: None,
      }),
      generate_log_action(
        &self.extractions,
        action,
        FieldsRef::new(&self.captured_fields, &self.matching_fields),
      )
    );
  }

  fn add_extracted_timestamp(&mut self, id: &str, timestamp: OffsetDateTime) {
    self
      .extractions
      .timestamps
      .get_or_insert_default()
      .insert(id.to_string(), timestamp);
  }

  fn add_extracted_field(&mut self, id: &str, value: &str) {
    self
      .extractions
      .fields
      .get_or_insert_default()
      .insert(id.to_string(), value.to_string());
  }

  fn add_field(&mut self, key: LogFieldKey, value: &str) {
    self.captured_fields.insert(key, value.into());
  }
}

#[test]
fn generate_log_no_fields() {
  let helper = Helper::new();
  helper.expect_log(
    "message",
    &[],
    LogType::Normal,
    &make_generate_log_action("message", &[], "id", LogType::Normal),
  );
}

#[test]
fn generate_log_with_saved_fields_math() {
  let action = make_generate_log_action(
    "hello world",
    &[
      (
        "subtract",
        TestFieldType::Subtract(
          TestFieldRef::SavedFieldId("id1"),
          TestFieldRef::SavedFieldId("id2"),
        ),
      ),
      (
        "add",
        TestFieldType::Add(
          TestFieldRef::SavedFieldId("id1"),
          TestFieldRef::SavedFieldId("id2"),
        ),
      ),
      (
        "multiply",
        TestFieldType::Multiply(
          TestFieldRef::SavedFieldId("id1"),
          TestFieldRef::SavedFieldId("id2"),
        ),
      ),
      (
        "divide",
        TestFieldType::Divide(
          TestFieldRef::SavedFieldId("id1"),
          TestFieldRef::SavedFieldId("id2"),
        ),
      ),
    ],
    "id",
    LogType::Normal,
  );
  let mut helper = Helper::new();
  helper.expect_log(
    "hello world",
    &[
      ("subtract", "NaN"),
      ("add", "NaN"),
      ("multiply", "NaN"),
      ("divide", "NaN"),
    ],
    LogType::Normal,
    &action,
  );
  helper.add_extracted_field("id1", "10.0");
  helper.add_extracted_field("id2", "12");
  helper.expect_log(
    "hello world",
    &[
      ("subtract", "-2"),
      ("add", "22"),
      ("multiply", "120"),
      ("divide", "0.8333333333333334"),
    ],
    LogType::Normal,
    &action,
  );
}

#[test]
fn generate_log_with_field_from_current_log() {
  let action = make_generate_log_action(
    "hello world",
    &[
      (
        "add_both_bad",
        TestFieldType::Subtract(
          TestFieldRef::SavedFieldId("bad1"),
          TestFieldRef::FieldFromCurrentLog("bad2"),
        ),
      ),
      (
        "add_1_bad",
        TestFieldType::Subtract(
          TestFieldRef::SavedFieldId("id1"),
          TestFieldRef::FieldFromCurrentLog("bad2"),
        ),
      ),
      (
        "add",
        TestFieldType::Subtract(
          TestFieldRef::SavedFieldId("id1"),
          TestFieldRef::FieldFromCurrentLog("id2"),
        ),
      ),
    ],
    "id",
    LogType::Normal,
  );
  let mut helper = Helper::new();
  helper.add_extracted_field("bad1", "not a number");
  helper.add_extracted_field("id1", "42");
  helper.add_field("bad2".into(), "also not a number");
  helper.add_field("id2".into(), "10.0");
  helper.expect_log(
    "hello world",
    &[("add_both_bad", "NaN"), ("add_1_bad", "NaN"), ("add", "32")],
    LogType::Normal,
    &action,
  );
}

#[test]
fn generate_log_with_saved_timestamp_math() {
  let action = make_generate_log_action(
    "hello world",
    &[
      (
        "single",
        TestFieldType::Single(TestFieldRef::Fixed("single_value")),
      ),
      (
        "subtract",
        TestFieldType::Subtract(
          TestFieldRef::SavedTimestampId("id2"),
          TestFieldRef::SavedTimestampId("id1"),
        ),
      ),
    ],
    "id",
    LogType::Span,
  );
  let mut helper = Helper::new();
  helper.expect_log(
    "hello world",
    &[("single", "single_value"), ("subtract", "NaN")],
    LogType::Span,
    &action,
  );
  helper.add_extracted_timestamp("id1", datetime!(2023-10-01 12:00:00 UTC));
  helper.add_extracted_timestamp("id2", datetime!(2023-10-01 12:00:00.003 UTC));
  helper.expect_log(
    "hello world",
    &[("single", "single_value"), ("subtract", "3")],
    LogType::Span,
    &action,
  );
}

#[test]
fn generate_log_with_uuid() {
  let action = make_generate_log_action(
    "hello world",
    &[("uuid", TestFieldType::Single(TestFieldRef::Uuid))],
    "id",
    LogType::Span,
  );
  let helper = Helper::new();
  let log = generate_log_action(
    &helper.extractions,
    &action,
    FieldsRef::new(&helper.captured_fields, &helper.matching_fields),
  )
  .unwrap();

  Uuid::parse_str(log.fields["uuid"].as_str().unwrap()).unwrap();
}
