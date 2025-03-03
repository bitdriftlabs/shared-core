use crate::generate_log::generate_log_action;
use crate::workflow::TraversalExtractions;
use bd_log_primitives::{log_level, FieldsRef, Log, LogField, LogFields, LogType};
use bd_proto::protos::workflow::workflow::workflow::action::action_generate_log::generated_field::Generated_field_value_type;
use bd_proto::protos::workflow::workflow::workflow::action::action_generate_log::value_reference::Value_reference_type;
use bd_proto::protos::workflow::workflow::workflow::action::action_generate_log::{
  GeneratedField,
  ValueReference,
  ValueReferencePair,
};
use bd_proto::protos::workflow::workflow::workflow::action::ActionGenerateLog;
use time::macros::datetime;
use time::OffsetDateTime;

struct Helper {
  now: OffsetDateTime,
  extractions: TraversalExtractions,
  captured_fields: LogFields,
  matching_fields: LogFields,
}

impl Helper {
  fn new(now: OffsetDateTime) -> Self {
    let extractions = TraversalExtractions::default();
    let captured_fields = LogFields::default();
    let matching_fields = LogFields::default();
    Self {
      now,
      extractions,
      captured_fields,
      matching_fields,
    }
  }

  fn expect_log(&self, message: &str, fields: &[(&str, &str)], action: &ActionGenerateLog) {
    assert_eq!(
      Some(Log {
        log_level: log_level::DEBUG,
        log_type: LogType::Normal,
        message: message.into(),
        fields: fields
          .iter()
          .map(|(k, v)| LogField {
            key: k.to_string(),
            value: (*v).into(),
          })
          .collect(),
        matching_fields: vec![],
        session_id: String::new(),
        occurred_at: self.now,
      }),
      generate_log_action(
        &self.extractions,
        &action,
        &FieldsRef::new(&self.captured_fields, &self.matching_fields),
        self.now
      )
    );
  }
}

#[test]
fn generate_log_no_fields() {
  let action = ActionGenerateLog {
    message: "hello world".to_string(),
    fields: vec![],
    ..Default::default()
  };
  let helper = Helper::new(datetime!(2023-10-01 12:00:00 UTC));
  helper.expect_log("hello world", &[], &action);
}

#[test]
fn generate_log_with_saved_timestamp_math() {
  let action = ActionGenerateLog {
    message: "hello world".to_string(),
    fields: vec![
      GeneratedField {
        name: "single".to_string(),
        generated_field_value_type: Some(Generated_field_value_type::Single(ValueReference {
          value_reference_type: Some(Value_reference_type::Fixed("single_value".to_string())),
          ..Default::default()
        })),
        ..Default::default()
      },
      GeneratedField {
        name: "subtract".to_string(),
        generated_field_value_type: Some(Generated_field_value_type::Subtract(
          ValueReferencePair {
            lhs: Some(ValueReference {
              value_reference_type: Some(Value_reference_type::SavedTimestampId("id1".to_string())),
              ..Default::default()
            }),
            lhs: Some(ValueReference {
              value_reference_type: Some(Value_reference_type::SavedTimestampId("id2".to_string())),
              ..Default::default()
            }),
            ..Default::default()
          },
        )),
        ..Default::default()
      },
    ],
    ..Default::default()
  };
  let helper = Helper::new(datetime!(2023-10-01 12:00:00 UTC));
  helper.expect_log("hello world", &[], &action);
}
