// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use base_log_matcher::{AnyMatch, MessageMatch, StringMatchType, TypeMatch};
use bd_proto::flatbuffers::buffer_log::bitdrift_public::fbs::logging::v_1::LogType;
use bd_proto::protos::bdtail::bdtail_config::{BdTailConfigurations, BdTailStream};
use bd_proto::protos::client::api::configuration_update::{StateOfTheWorld, Update_type};
use bd_proto::protos::client::api::ConfigurationUpdate;
use bd_proto::protos::client::matcher::root_matcher::{self, ClientTarget};
use bd_proto::protos::client::matcher::RootMatcher;
pub use bd_proto::protos::config::v1::config::buffer_config::Type as BufferType;
use bd_proto::protos::config::v1::config::buffer_config::{BufferFilter, BufferSizes};
use bd_proto::protos::config::v1::config::log_matcher::{
  base_log_matcher,
  BaseLogMatcher,
  Match_type,
};
use bd_proto::protos::filter::filter::{Filter, FiltersConfiguration};
use bd_proto::protos::workflow::workflow::{Workflow, WorkflowsConfiguration};
#[rustfmt::skip]
use crate::declare_transition;
use crate::workflow::macros::{
  action,
  all,
  any,
  log_matches,
  metric_value,
  not,
  rule,
  state,
  workflow_proto,
};
use bd_proto::protos::config::v1::config::{
  buffer_config,
  BufferConfig,
  BufferConfigList,
  LogMatcher as BufferLogMatcher,
};
use bd_proto::protos::log_matcher::log_matcher::LogMatcher;

// Matches logs with a specific log level(s) or log type.
macro_rules! matches {
  (level >= $level:expr) => {
    make_base_matcher(make_log_level_match(
      $level,
      base_log_matcher::log_level_match::ComparisonOperator::GREATER_THAN_OR_EQUAL,
    ))
  };
}

//
// BufferConfigBuilder
//

/// Simplifies constructing a buffer config.
pub struct BufferConfigBuilder {
  pub name: &'static str,
  pub buffer_type: buffer_config::Type,
  pub filter: Option<BufferLogMatcher>,
  pub non_volatile_size: u32,
  pub volatile_size: u32,
}

impl BufferConfigBuilder {
  #[must_use]
  pub fn build(self) -> BufferConfig {
    BufferConfig {
      name: self.name.to_string(),
      id: self.name.to_string(),
      filters: vec![BufferFilter {
        name: self.name.to_string(),
        id: self.name.to_string(),
        filter: self.filter.into(),
        ..Default::default()
      }],
      buffer_sizes: Some(BufferSizes {
        non_volatile_buffer_size_bytes: self.non_volatile_size,
        volatile_buffer_size_bytes: self.volatile_size,
        ..Default::default()
      })
      .into(),
      type_: self.buffer_type.into(),
      context_matcher: None.into(),
      ..Default::default()
    }
  }
}

// TODO(snowp): Add some docs here and clean up
#[must_use]
pub fn invalid_configuration() -> ConfigurationUpdate {
  ConfigurationUpdate {
    version_nonce: String::new(),
    update_type: None,
    ..Default::default()
  }
}

#[must_use]
pub fn default_buffer_config(
  buffer_type: buffer_config::Type,
  filter: Option<BufferLogMatcher>,
) -> BufferConfig {
  BufferConfigBuilder {
    name: "default",
    buffer_type,
    filter,
    non_volatile_size: 100_000,
    volatile_size: 10_000,
  }
  .build()
}

#[must_use]
pub fn default_buffer_drop_all_logs(buffer_type: buffer_config::Type) -> BufferConfig {
  BufferConfigBuilder {
    name: "default",
    buffer_type,
    filter: None,
    non_volatile_size: 100_000,
    volatile_size: 10_000,
  }
  .build()
}

#[must_use]
pub fn configuration_update(version: &str, sow: StateOfTheWorld) -> ConfigurationUpdate {
  ConfigurationUpdate {
    version_nonce: version.to_string(),
    update_type: Some(Update_type::StateOfTheWorld(sow)),
    ..Default::default()
  }
}

#[derive(Debug, Default)]
pub struct ConfigurationUpdateParts {
  pub buffer_config: Vec<BufferConfig>,
  pub workflows: Vec<Workflow>,
  pub bdtail_streams: Vec<BdTailStream>,
  pub filters_configuration: Vec<Filter>,
}

#[must_use]
pub fn configuration_update_from_parts(
  version: &str,
  parts: ConfigurationUpdateParts,
) -> ConfigurationUpdate {
  ConfigurationUpdate {
    version_nonce: version.to_string(),
    update_type: Some(Update_type::StateOfTheWorld(StateOfTheWorld {
      buffer_config_list: (!parts.buffer_config.is_empty())
        .then(|| BufferConfigList {
          buffer_config: parts.buffer_config,
          ..Default::default()
        })
        .into(),
      workflows_configuration: (!parts.workflows.is_empty())
        .then(|| WorkflowsConfiguration {
          workflows: parts.workflows,
          ..Default::default()
        })
        .into(),
      bdtail_configuration: (!parts.bdtail_streams.is_empty())
        .then(|| BdTailConfigurations {
          active_streams: parts.bdtail_streams,
          ..Default::default()
        })
        .into(),
      filters_configuration: (!parts.filters_configuration.is_empty())
        .then(|| FiltersConfiguration {
          filters: parts.filters_configuration,
          ..Default::default()
        })
        .into(),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
pub fn make_benchmarking_buffers_config() -> Vec<BufferConfig> {
  vec![make_buffer(
    "default_buffer_id",
    buffer_config::Type::TRIGGER,
    matches!(level >= base_log_matcher::log_level_match::LogLevel::TRACE),
  )]
}

#[must_use]
pub fn make_benchmarking_configuration_update() -> ConfigurationUpdate {
  configuration_update_from_parts(
    "1",
    ConfigurationUpdateParts {
      buffer_config: make_benchmarking_buffers_config(),
      ..Default::default()
    },
  )
}

#[must_use]
pub fn make_benchmarking_configuration_with_workflows_update() -> ConfigurationUpdate {
  let mut a = state!("a");
  let b = state!("b");

  declare_transition!(
    &mut a => &b;
    when rule!(
      all!(
        log_matches!(message == "SceneWillEnterFG"),
        log_matches!(tag("os") == "iOS"),
      )
    );
    do action!(emit_counter "app_open"; value metric_value!(1))
  );

  let workflow1 = workflow_proto!("1"; exclusive with a, b);

  let mut c = state!("c");
  let d = state!("d");

  declare_transition!(
    &mut c => &d;
    when rule!(
      any!(
        all!(
          log_matches!(message == "SceneDidEnterBG"),
          log_matches!(tag("os") == "iOS"),
        ),
        all!(
          log_matches!(message == "AppFinishedLaunching"),
          log_matches!(tag("os") == "iOS"),
        ),
      )
    );
    do action!(emit_counter "app_close"; value metric_value!(1))
  );

  let workflow2 = workflow_proto!("2"; exclusive with c, d);

  configuration_update_from_parts(
    "1",
    ConfigurationUpdateParts {
      buffer_config: make_benchmarking_buffers_config(),
      workflows: vec![workflow1, workflow2],
      ..Default::default()
    },
  )
}

#[must_use]
pub fn make_buffer_matcher_matching_everything() -> BufferLogMatcher {
  BufferLogMatcher {
    match_type: Some(Match_type::BaseMatcher(BaseLogMatcher {
      match_type: Some(base_log_matcher::Match_type::AnyMatch(AnyMatch::default())),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
pub fn make_buffer_matcher_matching_everything_except_internal_logs() -> BufferLogMatcher {
  BufferLogMatcher {
    match_type: Some(Match_type::NotMatcher(Box::new(BufferLogMatcher {
      match_type: Some(Match_type::BaseMatcher(BaseLogMatcher {
        match_type: Some(base_log_matcher::Match_type::TypeMatch(TypeMatch {
          type_: LogType::InternalSDK.0,
          ..Default::default()
        })),
        ..Default::default()
      })),
      ..Default::default()
    }))),
    ..Default::default()
  }
}

#[must_use]
pub fn make_buffer_matcher_matching_resource_logs() -> BufferLogMatcher {
  BufferLogMatcher {
    match_type: Some(Match_type::BaseMatcher(BaseLogMatcher {
      match_type: Some(base_log_matcher::Match_type::TypeMatch(TypeMatch {
        type_: LogType::Resource.0,
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
pub fn make_workflow_matcher_matching_everything_except_internal_logs() -> LogMatcher {
  not!(log_matches!(tag("log_type") == "0"))
}

#[must_use]
pub fn match_message(message: &str) -> BufferLogMatcher {
  BufferLogMatcher {
    match_type: Some(Match_type::BaseMatcher(BaseLogMatcher {
      match_type: Some(base_log_matcher::Match_type::MessageMatch(MessageMatch {
        match_type: StringMatchType::EXACT.into(),
        match_value: message.to_string(),
        ..Default::default()
      })),
      ..Default::default()
    })),
    ..Default::default()
  }
}

#[must_use]
pub fn make_configuration_update_with_workflow_flushing_buffer_on_anything(
  buffer_id: &str,
  buffer_type: buffer_config::Type,
) -> ConfigurationUpdate {
  let mut a = state!("a");
  let b = state!("b");

  declare_transition!(
    &mut a => &b;
    when rule!(
      any!(
        log_matches!(message == "foo"),
        not!(crate::log_matches!(message == "foo")),
      )
    );
    do action!(flush_buffers &[buffer_id]; id "flush_action_id")
  );

  let workflow = workflow_proto!("1"; exclusive with a, b);

  configuration_update_from_parts(
    "1",
    ConfigurationUpdateParts {
      buffer_config: make_buffer_config_matching_everything(buffer_id, buffer_type),
      workflows: vec![workflow],
      ..Default::default()
    },
  )
}

#[must_use]
pub fn make_configuration_update_with_workflow_flushing_buffer(
  buffer_id: &str,
  buffer_type: buffer_config::Type,
  buffer_matcher: BufferLogMatcher,
  workflow_matcher: LogMatcher,
) -> ConfigurationUpdate {
  let mut a = state!("a");
  let b = state!("b");

  declare_transition!(
    &mut a => &b;
    when rule!(
      workflow_matcher
    );
    do action!(flush_buffers &[buffer_id]; id "flush_action_id")
  );

  let workflow = workflow_proto!("1"; exclusive with a, b);

  configuration_update_from_parts(
    "1",
    ConfigurationUpdateParts {
      buffer_config: make_buffer_config(buffer_id, buffer_type, buffer_matcher),
      workflows: vec![workflow],
      ..Default::default()
    },
  )
}

#[must_use]
pub fn make_workflow_config_flushing_buffer(
  buffer_id: &str,
  matcher: bd_proto::protos::log_matcher::log_matcher::LogMatcher,
) -> Vec<Workflow> {
  let mut a = state!("a");
  let b = state!("b");

  declare_transition!(
    &mut a => &b;
    when rule!(
      matcher
    );
    do action!(flush_buffers &[buffer_id]; id "flush_action_id")
  );

  vec![workflow_proto!("1"; exclusive with a, b)]
}

#[must_use]
pub fn make_buffer_config_matching_everything(
  id: &str,
  buffer_type: buffer_config::Type,
) -> Vec<BufferConfig> {
  make_buffer_config(id, buffer_type, make_buffer_matcher_matching_everything())
}

pub fn make_buffer_config(
  id: &str,
  buffer_type: buffer_config::Type,
  matcher: BufferLogMatcher,
) -> Vec<BufferConfig> {
  vec![make_buffer(id, buffer_type, matcher)]
}

//
// Helper Methods
//

fn make_context_matcher() -> RootMatcher {
  RootMatcher {
    target_type: Some(root_matcher::Target_type::ClientTarget(
      ClientTarget::default(),
    )),
    matcher: protobuf::MessageField::some(bd_proto::protos::client::matcher::Matcher {
      type_: Some(bd_proto::protos::client::matcher::matcher::Type::Always(
        true,
      )),
      ..Default::default()
    }),
    ..Default::default()
  }
}

fn make_log_level_match(
  log_level: base_log_matcher::log_level_match::LogLevel,
  operator: base_log_matcher::log_level_match::ComparisonOperator,
) -> base_log_matcher::Match_type {
  base_log_matcher::Match_type::LogLevelMatch(base_log_matcher::LogLevelMatch {
    operator: operator.into(),
    log_level: log_level.into(),
    ..Default::default()
  })
}

fn make_base_matcher(
  match_type: base_log_matcher::Match_type,
) -> bd_proto::protos::config::v1::config::LogMatcher {
  bd_proto::protos::config::v1::config::LogMatcher {
    match_type: bd_proto::protos::config::v1::config::log_matcher::Match_type::BaseMatcher(
      bd_proto::protos::config::v1::config::log_matcher::BaseLogMatcher {
        match_type: match_type.into(),
        ..Default::default()
      },
    )
    .into(),
    ..Default::default()
  }
}

fn make_buffer(
  id: &str,
  buffer_type: buffer_config::Type,
  matcher: BufferLogMatcher,
) -> BufferConfig {
  BufferConfig {
    name: id.to_string(),
    id: id.to_string(),
    filters: vec![
      bd_proto::protos::config::v1::config::buffer_config::BufferFilter {
        name: id.to_string(),
        id: id.to_string(),
        filter: protobuf::MessageField::some(matcher),
        ..Default::default()
      },
    ],
    type_: ::protobuf::EnumOrUnknown::new(buffer_type),
    buffer_sizes: protobuf::MessageField::some(buffer_config::BufferSizes {
      volatile_buffer_size_bytes: 2_097_152,
      non_volatile_buffer_size_bytes: 5_242_880,
      ..Default::default()
    }),
    context_matcher: protobuf::MessageField::some(make_context_matcher()),
    ..Default::default()
  }
}
