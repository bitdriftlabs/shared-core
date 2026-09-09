source_info_gated_mod! {
  matcher => "with_source/matcher.rs",
  state_payload => "with_source/state_payload.rs",
  scope => "with_source/scope.rs",
}

pub mod payload {
  pub use super::state_payload::*;
  pub use crate::protos::logging::payload::Data;
}

use super::value_matcher::value_matcher;
use bd_pgv::generated::protos::validate;
