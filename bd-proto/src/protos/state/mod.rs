source_info_gated_mod! {
  matcher => "with_source/matcher.rs",
  state_payload => "with_source/state_payload.rs",
  scope => "with_source/scope.rs",
}

pub use self::state_payload as payload;
use super::value_matcher::value_matcher;
use bd_pgv::generated::protos::validate;
