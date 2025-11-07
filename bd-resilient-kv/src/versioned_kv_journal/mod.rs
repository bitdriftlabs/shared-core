use bd_proto::protos::state;

mod file_manager;
mod framing;
mod journal;
mod memmapped_journal;
pub mod recovery;
pub mod store;

/// Represents a value with its associated timestamp.
#[derive(Debug, Clone, PartialEq)]
pub struct TimestampedValue {
  /// The value stored in the key-value store.
  pub value: state::payload::StateValue,

  /// The timestamp (in microseconds since UNIX epoch) when this value was last written.
  pub timestamp: u64,
}

#[cfg(test)]
pub fn make_string_value(s: &str) -> state::payload::StateValue {
  state::payload::StateValue {
    value_type: Some(state::payload::state_value::Value_type::StringValue(
      s.to_string(),
    )),
    ..Default::default()
  }
}
