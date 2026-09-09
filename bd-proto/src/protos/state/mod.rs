source_info_gated_mod! {
  matcher => "with_source/matcher.rs",
  state_payload => "with_source/state_payload.rs",
  scope => "with_source/scope.rs",
}

// The generated state payload imports logging's `payload.proto` as `super::payload`.
// Keep the legacy state namespace while routing that import's descriptor to logging.
pub mod payload {
  pub use super::state_payload::{StateValue, state_value};
  pub use crate::protos::logging::payload::{Data, file_descriptor};
}

use super::value_matcher::value_matcher;
use bd_pgv::generated::protos::validate;
