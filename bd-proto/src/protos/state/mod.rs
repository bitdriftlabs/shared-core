#[cfg(not(feature = "with-source-info"))]
pub mod matcher;
#[cfg(feature = "with-source-info")]
#[path = "with_source/matcher.rs"]
pub mod matcher;

#[cfg(not(feature = "with-source-info"))]
pub mod payload;
#[cfg(feature = "with-source-info")]
#[path = "with_source/payload.rs"]
pub mod payload;

#[cfg(not(feature = "with-source-info"))]
pub mod scope;
#[cfg(feature = "with-source-info")]
#[path = "with_source/scope.rs"]
pub mod scope;

use super::value_matcher::value_matcher;
use bd_pgv::generated::protos::validate;
