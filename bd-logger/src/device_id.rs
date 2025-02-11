use bd_log_metadata::AnnotatedLogFields;
use bd_log_primitives::{AnnotatedLogField, LogInterceptor, LogLevel, LogMessage, LogType};

//
// DeviceIdInterceptor
//

pub struct DeviceIdInterceptor {
  device_id: String,
}

impl DeviceIdInterceptor {
  pub const fn new(device_id: String) -> Self {
    Self { device_id }
  }
}


impl LogInterceptor for DeviceIdInterceptor {
  fn process(
    &self,
    _log_level: LogLevel,
    _log_type: LogType,
    _msg: &LogMessage,
    _fields: &mut AnnotatedLogFields,
    matching_fields: &mut AnnotatedLogFields,
  ) {
    // The device_id field is populated by lapi on upload based on the handshake, so by putting
    // it in the matching fields it will be matchable and also present on all logs without us
    // having to add an explicit field that eats up buffer capacity.
    matching_fields.push(AnnotatedLogField::new_ootb(
      "_device_id".to_string(),
      self.device_id.clone().into(),
    ));
  }
}
