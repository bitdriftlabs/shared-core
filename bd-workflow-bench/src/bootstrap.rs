// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE.polyform file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#[cfg(test)]
#[path = "./bootstrap_test.rs"]
mod tests;

use anyhow::anyhow;
use bd_log_primitives::tiny_set::TinySet;
use bd_proto::protos::client::api::ApiResponse;
use bd_proto::protos::client::api::api_response::Response_type;
use bd_proto::protos::config::v1::config::buffer_config::Type;
use bd_workflows::config::WorkflowsConfiguration;
use bd_workflows::engine::WorkflowsEngineConfig;
use std::borrow::Cow;
use std::path::Path;

//
// Bootstrap
//

pub struct Bootstrap {
  pub config: WorkflowsEngineConfig,
  pub declared_workflow_count: usize,
  pub loaded_workflow_count: usize,
}

pub fn load_workflows_config(path: &Path) -> anyhow::Result<Bootstrap> {
  let contents = crate::fixtures::read_to_string(path)?;
  let response: ApiResponse = protobuf_json_mapping::parse_from_str(&contents)?;
  let Some(Response_type::ConfigurationUpdate(update)) = response.response_type else {
    return Err(anyhow!(
      "API response does not contain a configuration_update"
    ));
  };

  if !update.has_state_of_the_world() {
    return Err(anyhow!(
      "configuration update does not contain state_of_the_world"
    ));
  }

  let state = update.state_of_the_world();
  let workflows = state
    .workflows_configuration
    .as_ref()
    .ok_or_else(|| anyhow!("state_of_the_world does not contain workflows_configuration"))?;
  let debug_workflows = state
    .debug_workflows
    .as_ref()
    .map_or_else(Vec::new, |config| config.workflows.clone());
  let declared_workflow_count = workflows.workflows.len() + debug_workflows.len();
  let workflows_configuration =
    WorkflowsConfiguration::new(workflows.workflows.clone(), debug_workflows);
  let loaded_workflow_count = workflows_configuration.len();

  let (trigger_buffer_ids, continuous_buffer_ids) = state.buffer_config_list.as_ref().map_or_else(
    || (TinySet::default(), TinySet::default()),
    |buffer_list| {
      buffer_list.buffer_config.iter().fold(
        (TinySet::default(), TinySet::default()),
        |(mut trigger, mut continuous), buffer| {
          if buffer.type_.enum_value_or_default() == Type::CONTINUOUS {
            continuous.insert(Cow::Owned(buffer.id.clone()));
          } else {
            trigger.insert(Cow::Owned(buffer.id.clone()));
          }
          (trigger, continuous)
        },
      )
    },
  );

  Ok(Bootstrap {
    config: WorkflowsEngineConfig::new(
      workflows_configuration,
      trigger_buffer_ids,
      continuous_buffer_ids,
    ),
    declared_workflow_count,
    loaded_workflow_count,
  })
}
