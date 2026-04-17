source_info_gated_mod! {
  matcher => "with_source/matcher.rs",
  payload => "with_source/payload.rs",
  scope => "with_source/scope.rs",
}

use super::value_matcher::value_matcher;
use bd_pgv::generated::protos::validate;
