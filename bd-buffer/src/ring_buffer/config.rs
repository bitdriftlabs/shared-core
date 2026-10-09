use bd_proto::protos::config::v1::config::{BufferConfig, buffer_config};
use protobuf::EnumOrUnknown;

//
// BufferSettings
//

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BufferSettings {
  pub name: String,
  pub id: String,
  pub type_: EnumOrUnknown<buffer_config::Type>,
  pub volatile_buffer_size_bytes: u32,
  pub non_volatile_buffer_size_bytes: u32,
}

impl From<&BufferConfig> for BufferSettings {
  fn from(buffer: &BufferConfig) -> Self {
    Self {
      name: buffer.name.clone(),
      id: buffer.id.clone(),
      type_: buffer.type_,
      volatile_buffer_size_bytes: buffer
        .buffer_sizes
        .as_ref()
        .map_or(10_000, |sizes| sizes.volatile_buffer_size_bytes),
      non_volatile_buffer_size_bytes: buffer
        .buffer_sizes
        .as_ref()
        .map_or(100_000, |sizes| sizes.non_volatile_buffer_size_bytes),
    }
  }
}
