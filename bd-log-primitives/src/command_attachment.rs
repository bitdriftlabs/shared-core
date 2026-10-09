#[cfg(test)]
#[path = "./command_attachment_test.rs"]
mod tests;

use bd_macros::{ApproximateSize, proto_serializable};
use bd_proto::protos::logging::payload::CommandAttachment as ProtoCommandAttachment;
pub use bd_proto::protos::logging::payload::command_attachment::Source as AttachmentSource;
use bd_proto_util::serialization::runtime::Tag;
use protobuf::CodedInputStream;
use protobuf::rt::WireType;

//
// LogAttachment
//

#[derive(ApproximateSize, Clone, Debug, Default, PartialEq, Eq)]
#[proto_serializable(
  serialize_only,
  validate_against = "bd_proto::protos::logging::payload::CommandAttachment"
)]
pub struct LogAttachment {
  #[field(id = 1)]
  pub artifact_id: String,
  #[field(id = 2)]
  pub content_type: String,
  #[field(id = 3)]
  pub size_bytes: u64,
  #[field(id = 4, proto_enum)]
  #[approximate_size(skip)]
  pub source: AttachmentSource,
}

/// Reads attachment metadata without decoding the message, fields, or compressed contents.
pub fn extract_command_attachment(
  bytes: &[u8],
) -> anyhow::Result<Option<(String, ProtoCommandAttachment)>> {
  let mut stream = CodedInputStream::from_bytes(bytes);
  let mut session_id = String::new();
  let mut attachment = None;
  while let Some(raw_tag) = stream.read_raw_tag_or_eof()? {
    let tag = Tag::new(raw_tag)?;
    match tag.field_number {
      5 | 10 if tag.wire_type != WireType::LengthDelimited => {
        anyhow::bail!("incorrect attachment log wire type");
      },
      5 => session_id = stream.read_string()?,
      10 => attachment = Some(stream.read_message()?),
      _ => stream.skip_field(tag.wire_type)?,
    }
  }
  Ok(attachment.map(|attachment| (session_id, attachment)))
}
